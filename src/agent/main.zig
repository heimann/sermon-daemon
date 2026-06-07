const std = @import("std");
const collector_mod = @import("collector");
const logs_mod = @import("logs");
const rules_mod = @import("rules");
const proc_self_mod = @import("proc_self");
const proxmox_mod = @import("proxmox");
const push_mod = @import("push");
const storage_mod = @import("storage");

const default_db_path = "~/.local/share/sermon/metrics.db";
const default_config_path = "~/.config/sermon/config.json";
const default_interval: u64 = 10;
const default_retention: i64 = 7 * 24 * 60 * 60; // 7 days
const default_rules_filename = "log_rules.json";
// Periodic DuckDB handle refresh. Long soaks showed DuckDB can retain anonymous
// RSS after process-table writes/checkpoints; refreshing the embedded DB every
// few hours returns that memory without restarting the daemon process.
const default_storage_refresh_interval: u64 = 4 * 60 * 60;
const storage_refresh_min_interval: i64 = 15 * 60;
// Processes stored per cycle when config.max_processes is unset. Snapshotting
// every process is the dominant source of DB growth (millions of rows/week);
// keeping the top-N by CPU and memory cuts that ~5-15x. Set max_processes to 0
// to keep all (Collector.keep_all_processes).
const default_max_processes: u32 = 20;
// Rules live in their own file (not config.json) partly because the config
// loader reads into a fixed 4 KiB buffer; a rule set can be much larger.
const max_rules_file_bytes = 256 * 1024;

const Config = struct {
    db_path: ?[]const u8 = null,
    interval: ?u64 = null,
    retention: ?i64 = null,
    server_url: ?[]const u8 = null,
    api_key: ?[]const u8 = null,
    // DuckDB buffer-pool cap (MB). See Storage.default_memory_limit_mb.
    memory_limit_mb: ?u32 = null,
    // Max processes stored per cycle (top-N by CPU and memory). 0 keeps every
    // process (old behavior). See default_max_processes / keep_all_processes.
    max_processes: ?u32 = null,
    // Periodic DuckDB refresh interval in seconds. 0 disables time-based
    // refresh. See default_storage_refresh_interval.
    storage_refresh_interval: ?u64 = null,
    // RSS threshold in MB that can trigger a DuckDB refresh. 0 disables the RSS
    // trigger. Defaults to memory_limit_mb.
    storage_refresh_rss_mb: ?u32 = null,
};

fn loadConfig(allocator: std.mem.Allocator, config_path: []const u8) ?std.json.Parsed(Config) {
    const path = expandPath(allocator, config_path) catch return null;
    defer allocator.free(path);

    const file = std.fs.openFileAbsolute(path, .{}) catch return null;
    defer file.close();

    var buf: [4096]u8 = undefined;
    const len = file.readAll(&buf) catch return null;

    return std.json.parseFromSlice(Config, allocator, buf[0..len], .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    }) catch return null;
}

/// Load push-time log rules from `log_rules.json` next to the config file.
/// A missing file is silent (no rules); a present-but-unparseable file warns
/// and is ignored, so the daemon never half-applies a broken rule set.
fn loadRules(allocator: std.mem.Allocator, config_path: []const u8) ?std.json.Parsed(rules_mod.RuleFile) {
    const dir = std.fs.path.dirname(config_path) orelse return null;
    const rules_path_raw = std.fs.path.join(allocator, &.{ dir, default_rules_filename }) catch return null;
    defer allocator.free(rules_path_raw);

    const path = expandPath(allocator, rules_path_raw) catch return null;
    defer allocator.free(path);

    const file = std.fs.openFileAbsolute(path, .{}) catch return null;
    defer file.close();

    const stat = file.stat() catch return null;
    if (stat.size > max_rules_file_bytes) {
        std.debug.print("Warning: {s} exceeds {d} bytes - log rules ignored\n", .{ path, max_rules_file_bytes });
        return null;
    }

    const contents = allocator.alloc(u8, @intCast(stat.size)) catch return null;
    defer allocator.free(contents);
    const len = file.readAll(contents) catch return null;

    return std.json.parseFromSlice(rules_mod.RuleFile, allocator, contents[0..len], .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    }) catch |err| {
        std.debug.print("Warning: failed to parse {s}: {} - log rules disabled\n", .{ path, err });
        return null;
    };
}

var running: bool = true;

fn sigHandler(_: c_int) callconv(.c) void {
    running = false;
}

fn readConfigPathArg(allocator: std.mem.Allocator) !?[]const u8 {
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    _ = args.skip();

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--config")) {
            const path = args.next() orelse {
                std.debug.print("Error: --config requires a path\n", .{});
                std.process.exit(1);
            };
            return try allocator.dupe(u8, path);
        }
    }

    return null;
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const config_path_override = try readConfigPathArg(allocator);
    defer if (config_path_override) |path| allocator.free(path);
    const config_path = config_path_override orelse default_config_path;

    // Load config file (if it exists)
    const config = loadConfig(allocator, config_path);
    if (config == null and config_path_override != null) {
        std.debug.print("Error: could not load config file {s}\n", .{config_path});
        std.process.exit(1);
    }
    defer if (config) |c| c.deinit();

    // Push-time log rules live in log_rules.json next to the config file.
    // Optional - a missing file just means no filtering.
    const rules_parsed = loadRules(allocator, config_path);
    defer if (rules_parsed) |r| r.deinit();
    const log_rules: []const rules_mod.LogRule =
        if (rules_parsed) |r| r.value.log_rules else &.{};

    // Per-rule sample counters, parallel to log_rules, persisting across push
    // cycles so a "keep 1 in N" rule thins the whole stream rather than each
    // cycle in isolation.
    const sample_counts = try allocator.alloc(u64, log_rules.len);
    defer allocator.free(sample_counts);
    @memset(sample_counts, 0);
    const rule_set = rules_mod.RuleSet{ .rules = log_rules, .sample_counts = sample_counts };

    var db_path: []const u8 = if (config) |c| c.value.db_path orelse default_db_path else default_db_path;
    var interval: u64 = if (config) |c| c.value.interval orelse default_interval else default_interval;
    var server_url: ?[]const u8 = if (config) |c| c.value.server_url else null;
    var api_key: ?[]const u8 = if (config) |c| c.value.api_key else null;
    const memory_limit_mb: u32 = if (config) |c|
        c.value.memory_limit_mb orelse storage_mod.Storage.default_memory_limit_mb
    else
        storage_mod.Storage.default_memory_limit_mb;
    const max_processes: u32 = if (config) |c|
        c.value.max_processes orelse default_max_processes
    else
        default_max_processes;
    const storage_refresh_interval: u64 = if (config) |c|
        c.value.storage_refresh_interval orelse default_storage_refresh_interval
    else
        default_storage_refresh_interval;
    const storage_refresh_rss_mb: u32 = if (config) |c|
        c.value.storage_refresh_rss_mb orelse memory_limit_mb
    else
        memory_limit_mb;

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    _ = args.skip(); // skip program name

    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--config")) {
            _ = args.next() orelse {
                std.debug.print("Error: --config requires a path\n", .{});
                std.process.exit(1);
            };
        } else if (std.mem.eql(u8, arg, "--db")) {
            db_path = args.next() orelse {
                std.debug.print("Error: --db requires a path\n", .{});
                return;
            };
        } else if (std.mem.eql(u8, arg, "--interval")) {
            const val = args.next() orelse {
                std.debug.print("Error: --interval requires a value\n", .{});
                return;
            };
            interval = std.fmt.parseInt(u64, val, 10) catch {
                std.debug.print("Error: invalid interval '{s}'\n", .{val});
                return;
            };
        } else if (std.mem.eql(u8, arg, "--server")) {
            server_url = args.next() orelse {
                std.debug.print("Error: --server requires a URL\n", .{});
                return;
            };
        } else if (std.mem.eql(u8, arg, "--key")) {
            api_key = args.next() orelse {
                std.debug.print("Error: --key requires a token\n", .{});
                return;
            };
        } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            std.debug.print(
                \\Usage: sermon-agent [OPTIONS]
                \\
                \\Options:
                \\  --config <path>     Config file path (default: ~/.config/sermon/config.json)
                \\  --db <path>         Database path (default: ~/.local/share/sermon/metrics.db)
                \\  --interval <secs>   Collection interval in seconds (default: 10)
                \\  --server <url>      Push metrics to web server (optional)
                \\  --key <token>       Ingestion key for web server (optional)
                \\  -h, --help          Show this help
                \\
            , .{});
            return;
        }
    }

    if (server_url != null and api_key == null) {
        std.debug.print("Warning: --server is set but --key is missing - remote push is disabled\n", .{});
    }
    if (server_url == null and api_key != null) {
        std.debug.print("Warning: --key is set but --server is missing - remote push is disabled\n", .{});
    }

    // Expand ~ in db path
    const final_db_path = try expandPath(allocator, db_path);
    defer allocator.free(final_db_path);

    // Ensure parent directory exists (only for absolute paths like the default ~/.local/share/sermon/)
    if (std.fs.path.dirname(final_db_path)) |dir| {
        if (std.fs.path.isAbsolute(dir)) {
            std.fs.makeDirAbsolute(dir) catch |err| {
                if (err != error.PathAlreadyExists) {
                    std.debug.print("Warning: could not create directory {s}: {}\n", .{ dir, err });
                }
            };
        }
    }

    // Install signal handlers for graceful shutdown
    const sa = std.posix.Sigaction{
        .handler = .{ .handler = sigHandler },
        .mask = .{0} ** @typeInfo(std.posix.sigset_t).array.len,
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &sa, null);
    std.posix.sigaction(std.posix.SIG.TERM, &sa, null);

    // Open storage once for the daemon's lifetime. Reopening per-cycle
    // mmaps the entire DB on every collection tick and pegs CPU on
    // populated databases (~400 MB and up). See scripts/bench/.
    var storage = try storage_mod.Storage.initWithMemoryLimit(allocator, final_db_path, memory_limit_mb);
    defer storage.deinit();

    std.debug.print("sermon-agent started (db={s}, interval={d}s, memory_limit={d}MB, max_processes={d}, storage_refresh_interval={d}s, storage_refresh_rss={d}MB)\n", .{ final_db_path, interval, memory_limit_mb, max_processes, storage_refresh_interval, storage_refresh_rss_mb });
    if (log_rules.len > 0) {
        std.debug.print("loaded {d} log rule(s)\n", .{log_rules.len});
    }

    // Initialize collector. max_processes caps how many processes each cycle
    // stores (top-N by CPU and memory); set on the collector so both the
    // baseline and loop calls to collectProcesses trim identically.
    var coll = try collector_mod.Collector.init(allocator);
    coll.max_processes = max_processes;
    defer coll.deinit();

    const hostname = try readHostname(allocator);
    defer allocator.free(hostname);

    // Detect Proxmox runtime context. Held across the loop so per-cycle code
    // (inventory in V1 item 2, per-CT metrics in item 3) can branch on it
    // without re-shelling out to pveversion / re-reading /proc/self/cgroup.
    var runtime = proxmox_mod.detectRuntime(allocator);
    defer runtime.deinit(allocator);
    switch (runtime) {
        .not_proxmox => {},
        .host => |h| std.debug.print(
            "proxmox.detect: host node={s} runtime_version={s}\n",
            .{ h.node, h.runtime_version },
        ),
        .container => |c| std.debug.print(
            "proxmox.detect: container vmid={d}\n",
            .{c.vmid},
        ),
    }

    // Initialize log tailer (systemd journal)
    const log_sources = [_]logs_mod.LogSource{.systemd};
    var log_tailer: ?logs_mod.LogTailer = null;
    if (logs_mod.LogTailer.init(allocator, &log_sources)) |lt| {
        log_tailer = lt;
    } else |err| {
        std.debug.print("Warning: log tailer init failed (journalctl may not be available): {}\n", .{err});
    }
    defer if (log_tailer) |*lt| lt.deinit();

    // First sample establishes baseline for CPU deltas, then sleep so first real data is meaningful
    _ = try coll.collectMetrics();
    {
        const baseline_procs = try coll.collectProcesses(allocator);
        for (baseline_procs) |p| {
            allocator.free(p.name);
            allocator.free(p.cmdline);
            allocator.free(p.username);
            allocator.free(p.cgroup);
            allocator.free(p.unit);
        }
        allocator.free(baseline_procs);
    }
    std.Thread.sleep(1 * std.time.ns_per_s);

    // Run retention once on startup to trim a pre-wedged DB before the main
    // loop begins. Non-fatal: the main loop's failsafe handles persistent
    // storage failures; log once and continue.
    {
        const retention = if (config) |c| c.value.retention orelse default_retention else default_retention;
        storage.runRetention(retention) catch |err| {
            std.debug.print("Warning: startup retention failed: {}\n", .{err});
        };
    }

    var retention_counter: u64 = 0;
    var last_storage_refresh: i64 = std.time.timestamp();
    // Tracks whether a reconnect has been attempted since the last successful
    // write. Used for two-stage recovery: reconnect once, then quarantine.
    var reconnect_attempted: bool = false;
    var self_state = proc_self_mod.State.init();
    var ct_metrics_state = proxmox_mod.ContainerMetricsState.init(allocator);
    defer ct_metrics_state.deinit();

    // Main collection loop
    while (running) {
        const now = std.time.timestamp();

        // Collect data (no DB lock held)
        const metrics = coll.collectMetrics() catch |err| {
            std.debug.print("Warning: metrics collection failed: {}\n", .{err});
            continue;
        };
        const procs = coll.collectProcesses(allocator) catch |err| {
            std.debug.print("Warning: process collection failed: {}\n", .{err});
            continue;
        };
        defer {
            for (procs) |p| {
                allocator.free(p.name);
                allocator.free(p.cmdline);
                allocator.free(p.username);
                allocator.free(p.cgroup);
                allocator.free(p.unit);
            }
            allocator.free(procs);
        }
        const disks = collector_mod.Collector.collectDisks(allocator) catch |err| {
            std.debug.print("Warning: disk collection failed: {}\n", .{err});
            continue;
        };
        defer {
            for (disks) |d| {
                allocator.free(d.mount_point);
                allocator.free(d.filesystem);
            }
            allocator.free(disks);
        }

        // Container inventory: only on Proxmox hosts. Failures are non-fatal
        // and intentionally not warning-logged per cycle (would spam the
        // journal on a wedged Corosync ring); empty slice means "skip
        // containers this cycle, hosted side will keep last-known rows."
        const containers: []proxmox_mod.ContainerEntry = blk: {
            switch (runtime) {
                .host => {
                    break :blk proxmox_mod.collectInventory(allocator) catch |err| inv_err: {
                        std.log.warn("proxmox.inventory: collection failed: {}", .{err});
                        break :inv_err &[_]proxmox_mod.ContainerEntry{};
                    };
                },
                else => break :blk &[_]proxmox_mod.ContainerEntry{},
            }
        };
        defer if (containers.len > 0) proxmox_mod.freeContainers(allocator, containers);

        // Per-container CPU/memory metrics. Only populated when we have a
        // running container; first cycle for each new CT yields cpu_pct=NaN
        // (no prior delta), serialized as JSON null downstream.
        const ct_metrics: []proxmox_mod.ContainerMetrics = if (containers.len > 0)
            proxmox_mod.collectContainerMetrics(allocator, &ct_metrics_state, containers) catch |err| ctm_err: {
                std.log.warn("proxmox.container_metrics: collection failed: {}", .{err});
                break :ctm_err &[_]proxmox_mod.ContainerMetrics{};
            }
        else
            &[_]proxmox_mod.ContainerMetrics{};
        defer if (ct_metrics.len > 0) allocator.free(ct_metrics);

        var push_logs = std.ArrayList(logs_mod.LogEntry){};
        defer {
            for (push_logs.items) |*entry| {
                entry.deinit(allocator);
            }
            push_logs.deinit(allocator);
        }

        // Write everything (storage held open across cycles)
        {
            // Firehose guard: after the first insert failure in a cycle, skip
            // all remaining DB writes for that cycle. This caps failure logging
            // at one line per cycle rather than up to 1000 (one per log entry).
            // Logs are still drained from the tailer for the push path.
            var storage_failed = false;

            if (!storage_failed) {
                storage.insertMetrics(now, metrics) catch |err| {
                    std.debug.print("Warning: metrics insert failed: {}\n", .{err});
                    storage_failed = true;
                };
            }
            if (!storage_failed and procs.len > 0) {
                storage.insertProcesses(now, procs) catch |err| {
                    std.debug.print("Warning: process insert failed: {}\n", .{err});
                    storage_failed = true;
                };
            }
            if (!storage_failed and disks.len > 0) {
                storage.insertDisks(now, disks) catch |err| {
                    std.debug.print("Warning: disk insert failed: {}\n", .{err});
                    storage_failed = true;
                };
            }
            if (!storage_failed and containers.len > 0) {
                storage.insertContainers(now, containers) catch |err| {
                    std.debug.print("Warning: container insert failed: {}\n", .{err});
                    storage_failed = true;
                };
            }
            if (!storage_failed and ct_metrics.len > 0) {
                storage.insertContainerMetrics(now, ct_metrics) catch |err| {
                    std.debug.print("Warning: container_metrics insert failed: {}\n", .{err});
                    storage_failed = true;
                };
            }

            // Drain available log entries. Push path is independent of storage
            // health; only skip DB inserts when storage failed this cycle.
            if (log_tailer) |*lt| {
                var log_count: u32 = 0;
                while (log_count < 1000) : (log_count += 1) {
                    const maybe_entry = lt.next() catch break;
                    if (maybe_entry == null) break;
                    const entry = maybe_entry.?;
                    if (!storage_failed) {
                        storage.insertLog(entry) catch |err| {
                            std.debug.print("Warning: log insert failed: {}\n", .{err});
                            storage_failed = true;
                        };
                    }
                    push_logs.append(allocator, entry) catch |err| {
                        var owned_entry = entry;
                        owned_entry.deinit(allocator);
                        return err;
                    };
                }
            }

            // Run retention cleanup every hour
            retention_counter += interval;
            if (retention_counter >= 3600) {
                const retention = if (config) |c| c.value.retention orelse default_retention else default_retention;
                storage.runRetention(retention) catch |err| {
                    std.debug.print("Warning: retention cleanup failed: {}\n", .{err});
                };
                retention_counter = 0;
            }

            // Two-stage recovery when inserts consistently fail:
            //   Stage 1 - reconnect: clears the connection. Handles transient
            //             wedges and resets the counter on success. Sets
            //             reconnect_attempted so we don't reconnect in a loop.
            //   Stage 2 - quarantine: if reconnect already succeeded but inserts
            //             still fail (DB too large for memory_limit), rename the
            //             DB/WAL aside and open a fresh one. Also used immediately
            //             when reconnect itself fails (handles already null).
            // Reset reconnect_attempted whenever storage is healthy.
            if (storage.consecutive_insert_failures == 0) {
                reconnect_attempted = false;
            } else if (storage.consecutive_insert_failures >= storage_mod.Storage.reconnect_failure_threshold) {
                if (!reconnect_attempted) {
                    std.log.warn(
                        "DuckDB inserts failed {d} cycles in a row, reconnecting",
                        .{storage.consecutive_insert_failures},
                    );
                    if (storage.refresh()) |_| {
                        storage.consecutive_insert_failures = 0;
                        reconnect_attempted = true;
                        last_storage_refresh = now;
                    } else |err| {
                        // Reconnect failed (DB likely permanently wedged).
                        // Go straight to quarantine rather than looping on
                        // null handles.
                        std.log.warn("DuckDB reconnect failed: {}, quarantining wedged DB", .{err});
                        if (storage.quarantineAndReopen()) |_| {
                            storage.consecutive_insert_failures = 0;
                            reconnect_attempted = false;
                            last_storage_refresh = now;
                        } else |qerr| {
                            std.log.err("DuckDB quarantine failed: {}, exiting for restart", .{qerr});
                            running = false;
                        }
                    }
                } else {
                    // Reconnect succeeded but inserts are still failing:
                    // the same DB is too large/wedged. Quarantine it.
                    std.log.warn(
                        "DuckDB still unwritable after reconnect ({d} failures), quarantining",
                        .{storage.consecutive_insert_failures},
                    );
                    if (storage.quarantineAndReopen()) |_| {
                        storage.consecutive_insert_failures = 0;
                        reconnect_attempted = false;
                        last_storage_refresh = now;
                    } else |err| {
                        std.log.err("DuckDB quarantine failed: {}, exiting for restart", .{err});
                        running = false;
                    }
                }
            }
        }

        const self_sample = proc_self_mod.sample(&self_state) catch |err| sblk: {
            std.debug.print("Warning: proc_self sample failed: {}\n", .{err});
            break :sblk proc_self_mod.Sample{
                .cpu_percent = 0,
                .rss_kb = 0,
                .vsize_kb = 0,
                .threads = 0,
                .voluntary_ctxt_switches = 0,
                .nonvoluntary_ctxt_switches = 0,
                .uptime_seconds = 0,
            };
        };

        if (storage.consecutive_insert_failures == 0 and shouldRefreshStorage(
            now,
            last_storage_refresh,
            storage_refresh_interval,
            self_sample.rss_kb,
            storage_refresh_rss_mb,
        )) {
            const elapsed = now - last_storage_refresh;
            std.log.warn(
                "DuckDB storage refresh triggered (elapsed={d}s, rss={d}KB)",
                .{ elapsed, self_sample.rss_kb },
            );
            if (storage.refresh()) |_| {
                storage.consecutive_insert_failures = 0;
                reconnect_attempted = false;
                last_storage_refresh = now;
            } else |err| {
                std.log.warn("DuckDB storage refresh failed: {}, quarantining wedged DB", .{err});
                if (storage.quarantineAndReopen()) |_| {
                    storage.consecutive_insert_failures = 0;
                    reconnect_attempted = false;
                    last_storage_refresh = now;
                } else |qerr| {
                    std.log.err("DuckDB quarantine after refresh failed: {}, exiting for restart", .{qerr});
                    running = false;
                }
            }
        }

        if (server_url) |url| {
            if (api_key) |key| {
                const maybe_payload = push_mod.buildPayload(
                    allocator,
                    hostname,
                    now,
                    metrics,
                    procs,
                    disks,
                    push_logs.items,
                    rule_set,
                    self_sample,
                    storage.consecutive_insert_failures,
                    storage.dbSizeBytes(),
                    runtime,
                    containers,
                    ct_metrics,
                ) catch |err| blk: {
                    std.debug.print("Warning: payload build failed: {}\n", .{err});
                    break :blk null;
                };

                if (maybe_payload) |payload| {
                    defer allocator.free(payload);
                    push_mod.pushMetrics(allocator, url, key, payload) catch |err| {
                        std.debug.print("Warning: metrics push failed: {}\n", .{err});
                    };
                }
            }
        }

        // Sleep until next interval (interruptible)
        var remaining: u64 = interval;
        while (remaining > 0 and running) {
            std.Thread.sleep(1 * std.time.ns_per_s);
            remaining -= 1;
        }
    }

    std.debug.print("sermon-agent shutting down\n", .{});
}

fn shouldRefreshStorage(now: i64, last_refresh: i64, interval_seconds: u64, rss_kb: u64, rss_threshold_mb: u32) bool {
    const elapsed = now - last_refresh;
    if (elapsed < storage_refresh_min_interval) return false;

    if (interval_seconds > 0 and elapsed >= @as(i64, @intCast(interval_seconds))) {
        return true;
    }

    if (rss_threshold_mb > 0 and rss_kb >= @as(u64, rss_threshold_mb) * 1024) {
        return true;
    }

    return false;
}

fn readHostname(allocator: std.mem.Allocator) ![]const u8 {
    const file = std.fs.openFileAbsolute("/etc/hostname", .{}) catch {
        return allocator.dupe(u8, "unknown");
    };
    defer file.close();

    var buf: [256]u8 = undefined;
    const len = file.readAll(&buf) catch {
        return allocator.dupe(u8, "unknown");
    };

    const hostname = std.mem.trim(u8, buf[0..len], " \n\t\r");
    if (hostname.len == 0) {
        return allocator.dupe(u8, "unknown");
    }

    return allocator.dupe(u8, hostname);
}

fn expandPath(allocator: std.mem.Allocator, path: []const u8) ![]const u8 {
    if (path.len > 0 and path[0] == '~') {
        const home = std.posix.getenv("HOME") orelse return error.NoHomeDir;
        return std.fmt.allocPrint(allocator, "{s}{s}", .{ home, path[1..] });
    }
    return allocator.dupe(u8, path);
}
