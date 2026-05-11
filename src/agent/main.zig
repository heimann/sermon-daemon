const std = @import("std");
const collector_mod = @import("collector");
const logs_mod = @import("logs");
const proc_self_mod = @import("proc_self");
const push_mod = @import("push");
const storage_mod = @import("storage");

const default_db_path = "~/.local/share/sermon/metrics.db";
const default_config_path = "~/.config/sermon/config.json";
const default_interval: u64 = 10;
const default_retention: i64 = 7 * 24 * 60 * 60; // 7 days

const Config = struct {
    db_path: ?[]const u8 = null,
    interval: ?u64 = null,
    retention: ?i64 = null,
    server_url: ?[]const u8 = null,
    api_key: ?[]const u8 = null,
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

    var db_path: []const u8 = if (config) |c| c.value.db_path orelse default_db_path else default_db_path;
    var interval: u64 = if (config) |c| c.value.interval orelse default_interval else default_interval;
    var server_url: ?[]const u8 = if (config) |c| c.value.server_url else null;
    var api_key: ?[]const u8 = if (config) |c| c.value.api_key else null;

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
    var storage = try storage_mod.Storage.init(allocator, final_db_path);
    defer storage.deinit();

    std.debug.print("sermon-agent started (db={s}, interval={d}s)\n", .{ final_db_path, interval });

    // Initialize collector
    var coll = try collector_mod.Collector.init(allocator);
    defer coll.deinit();

    const hostname = try readHostname(allocator);
    defer allocator.free(hostname);

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
        }
        allocator.free(baseline_procs);
    }
    std.Thread.sleep(1 * std.time.ns_per_s);

    var retention_counter: u64 = 0;
    var self_state = proc_self_mod.State.init();

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

        var push_logs = std.ArrayList(logs_mod.LogEntry){};
        defer {
            for (push_logs.items) |*entry| {
                entry.deinit(allocator);
            }
            push_logs.deinit(allocator);
        }

        // Write everything (storage held open across cycles)
        {
            storage.insertMetrics(now, metrics) catch |err| {
                std.debug.print("Warning: metrics insert failed: {}\n", .{err});
            };
            if (procs.len > 0) {
                storage.insertProcesses(now, procs) catch |err| {
                    std.debug.print("Warning: process insert failed: {}\n", .{err});
                };
            }
            if (disks.len > 0) {
                storage.insertDisks(now, disks) catch |err| {
                    std.debug.print("Warning: disk insert failed: {}\n", .{err});
                };
            }

            // Drain available log entries
            if (log_tailer) |*lt| {
                var log_count: u32 = 0;
                while (log_count < 1000) : (log_count += 1) {
                    const maybe_entry = lt.next() catch break;
                    if (maybe_entry == null) break;
                    const entry = maybe_entry.?;
                    storage.insertLog(entry) catch |err| {
                        std.debug.print("Warning: log insert failed: {}\n", .{err});
                    };
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

            // If inserts have been failing in a row, the persistent connection is
            // probably wedged. Tear it down and re-init.
            if (storage.consecutive_insert_failures >= storage_mod.Storage.reconnect_failure_threshold) {
                std.log.warn(
                    "DuckDB inserts failed {d} cycles in a row, reconnecting",
                    .{storage.consecutive_insert_failures},
                );
                storage.reconnect() catch |err| {
                    std.log.err("DuckDB reconnect failed: {}", .{err});
                };
                storage.consecutive_insert_failures = 0;
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
                    self_sample,
                    storage.consecutive_insert_failures,
                    storage.dbSizeBytes(),
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
