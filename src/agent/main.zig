const std = @import("std");
const collector_mod = @import("collector");
const logs_mod = @import("logs");
const rules_mod = @import("rules");
const proc_self_mod = @import("proc_self");
const proxmox_mod = @import("proxmox");
const push_mod = @import("push");
const storage_mod = @import("storage");
// Parquet hot tier (plan 25 cutover). The daemon WRITE path is now a durable
// append-log (staging) periodically rolled to parquet (roll); the resident
// DuckDB write path (storage_mod) is retired here. storage_mod is still imported
// only for its back-compat constants (default_memory_limit_mb) - see below.
const staging_mod = @import("staging");
const roll_mod = @import("roll");

const default_db_path = "~/.local/share/sermon/metrics.db";
const default_config_path = "~/.config/sermon/config.json";
const default_interval: u64 = 10;
const default_retention: i64 = 7 * 24 * 60 * 60; // 7 days
const default_rules_filename = "log_rules.json";
// Roll trigger thresholds for the parquet hot tier. A table's staging segment
// rolls to parquet when it exceeds default_roll_max_bytes OR when
// default_roll_interval_s have elapsed since the last roll (so low-volume tables
// - disks, containers - still roll and don't sit un-rolled forever). Both are
// config-overridable (roll_max_bytes / roll_interval_s).
const default_roll_max_bytes: u64 = 8 * 1024 * 1024; // 8 MiB
// 1 hour: low-volume tables roll hourly instead of every 5 min, keeping the
// steady-state file count modest (~1-1.5k files over the 7-day retention window)
// now that compaction is a deferred follow-up. The 8 MiB size trigger still
// rolls busy tables sooner.
const default_roll_interval_s: u64 = 3600; // 1 hour
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
    // DuckDB buffer-pool cap (MB). RETAINED FOR BACK-COMPAT: the resident-DuckDB
    // write path is retired (plan 25), so this no longer caps a resident DB. The
    // roll uses a transient in-memory DuckDB whose lifetime is one roll; roll.zig
    // does not currently expose a buffer-pool knob, so this value is parsed (so
    // existing config.json files don't break) and logged but otherwise unused.
    memory_limit_mb: ?u32 = null,
    // Max processes stored per cycle (top-N by CPU and memory). 0 keeps every
    // process (old behavior). See default_max_processes / keep_all_processes.
    max_processes: ?u32 = null,
    // RETAINED FOR BACK-COMPAT ONLY: periodic DuckDB refresh interval. There is
    // no resident DuckDB to refresh after the cutover; parsed-but-unused.
    storage_refresh_interval: ?u64 = null,
    // RETAINED FOR BACK-COMPAT ONLY: RSS-triggered DuckDB refresh threshold.
    // No resident DuckDB to refresh; parsed-but-unused.
    storage_refresh_rss_mb: ?u32 = null,
    // Roll trigger: a table's staging segment rolls to parquet once it exceeds
    // this many bytes. See default_roll_max_bytes.
    roll_max_bytes: ?u64 = null,
    // Roll trigger: a table also rolls once this many seconds have elapsed since
    // its last roll, so low-volume tables don't sit un-rolled. See
    // default_roll_interval_s.
    roll_interval_s: ?u64 = null,
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
    // RETAINED FOR BACK-COMPAT: still parsed so old config.json files load, but
    // the resident-DuckDB write path it capped is retired. Logged at startup.
    const memory_limit_mb: u32 = if (config) |c|
        c.value.memory_limit_mb orelse storage_mod.Storage.default_memory_limit_mb
    else
        storage_mod.Storage.default_memory_limit_mb;
    const max_processes: u32 = if (config) |c|
        c.value.max_processes orelse default_max_processes
    else
        default_max_processes;
    // Roll trigger thresholds (parquet hot tier). Config-overridable.
    const roll_max_bytes: u64 = if (config) |c|
        c.value.roll_max_bytes orelse default_roll_max_bytes
    else
        default_roll_max_bytes;
    const roll_interval_s: u64 = if (config) |c|
        c.value.roll_interval_s orelse default_roll_interval_s
    else
        default_roll_interval_s;

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

    // ── Parquet hot tier root (plan 25 cutover) ──
    // The hot tier lives in the directory that USED to hold metrics.db (so
    // existing --db/db_path config keeps pointing at the same place). The
    // staging append-log lands in <root>/_staging/ and rolled parquet in
    // <root>/<table>/date=.../hour=.../*.parquet. We derive the root from the
    // configured db path's directory; if the path has no dir component we fall
    // back to the current directory.
    const root = try allocator.dupe(u8, std.fs.path.dirname(final_db_path) orelse ".");
    defer allocator.free(root);

    // STARTUP LIFECYCLE (must run BEFORE any append/roll/query):
    //   1. recoverOrphanTemps: a crash BETWEEN a roll's staging-reset and its
    //      rename leaves a durable `<seq>.parquet.tmp` whose rows are already
    //      gone from staging. Rename such orphans to their final name so those
    //      rows are not stranded. Idempotent; safe when there are none.
    roll_mod.recoverOrphanTemps(allocator, root) catch |err| {
        std.debug.print("Warning: orphan-temp recovery failed: {}\n", .{err});
    };

    //   1b. recoverCompactions: finish or roll back any day-compaction a crash
    //       interrupted. A committed `<seq>.manifest` replays its named-input
    //       deletes + publish exactly; an orphan `<seq>.building` (pre-commit
    //       crash) is dropped with its inputs left intact. Must run AFTER orphan-
    //       temp recovery (so a re-published roll temp is a candidate input next
    //       tick) and BEFORE any roll/query. Idempotent; no-op when there's none.
    //
    //       FATAL on failure: a committed manifest mid-replay can leave rows
    //       hidden in a `<seq>.building` that queries ignore (deleting inputs
    //       precedes publishing the building). Proceeding would silently serve a
    //       short count. Aborting lets a restart retry recovery from the durable
    //       manifest instead, so no row is ever stranded in a half-published merge.
    try roll_mod.recoverCompactions(allocator, root);

    // One-shot MIGRATION: if a legacy resident metrics.db still exists at the
    // old path, COPY its rows into the parquet tree (best-effort) and rename it
    // to metrics.db.migrated (kept for rollback). A failure is logged and the
    // daemon continues - the cloud holds the long-term history.
    if (roll_mod.migrateLegacyDb(allocator, root, final_db_path)) |migrated| {
        if (migrated) std.debug.print("migrated legacy {s} into parquet hot tier\n", .{final_db_path});
    } else |err| {
        std.debug.print("Warning: legacy db migration failed: {}\n", .{err});
    }

    // Open the durable staging append-log for the daemon's lifetime. This
    // replaces the resident DuckDB write path entirely.
    var stg = try staging_mod.Staging.open(allocator, root);
    defer stg.deinit();

    // REPLAY: re-roll any staging segments left over from a prior run so a crash
    // mid-run does not strand un-rolled rows in staging (and so the segment
    // doesn't keep growing past its size trigger across restarts). rollAll is a
    // no-op for empty segments.
    {
        const replayed = roll_mod.rollAll(allocator, root, &stg) catch |err| blk: {
            std.debug.print("Warning: startup replay roll failed: {}\n", .{err});
            break :blk 0;
        };
        if (replayed > 0) std.debug.print("startup replay: rolled {d} leftover staging segment(s)\n", .{replayed});
    }

    // Obsolete-config visibility: the resident-DuckDB refresh knobs are parsed for
    // back-compat but do nothing post-cutover. If an operator still has either set,
    // warn once so they aren't misled into thinking it's taking effect.
    if (config) |c| {
        if (c.value.storage_refresh_interval != null or c.value.storage_refresh_rss_mb != null) {
            std.log.warn("config: storage_refresh_interval / storage_refresh_rss_mb are obsolete post-cutover (no resident DuckDB to refresh) - ignored", .{});
        }
    }

    std.debug.print("sermon-agent started (root={s}, interval={d}s, max_processes={d}, roll_max_bytes={d}, roll_interval_s={d}s) [memory_limit_mb={d} retained for back-compat, unused]\n", .{ root, interval, max_processes, roll_max_bytes, roll_interval_s, memory_limit_mb });
    if (log_rules.len > 0) {
        std.debug.print("loaded {d} log rule(s)\n", .{log_rules.len});
    }

    // Per-table wall-clock of the last roll, so the time-based roll trigger fires
    // even for low-volume tables that never reach the byte threshold. Seeded to
    // "now" so a fresh start waits a full interval before the first time-roll.
    var last_roll: [staging_mod.Table.all.len]i64 = undefined;
    {
        const start_ts = std.time.timestamp();
        for (&last_roll) |*t| t.* = start_ts;
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

    // Run retention once on startup to trim stale partitions before the loop
    // begins. Non-fatal: best-effort, log once and continue.
    {
        const retention = if (config) |c| c.value.retention orelse default_retention else default_retention;
        roll_mod.runRetention(allocator, root, retention) catch |err| {
            std.debug.print("Warning: startup retention failed: {}\n", .{err});
        };
    }

    var retention_counter: u64 = 0;
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

        // ── WRITE PATH: append this cycle to the durable staging log ──
        // The resident DuckDB write path is retired (plan 25). One collect cycle
        // = one bracketed begin/end (takes the EX roll lock, appends each
        // non-empty table's record, fdatasyncs once, releases). The append guards
        // mirror today's storage.insert* guards (only write non-empty tables).
        {
            // Firehose guard, preserved in spirit: on the FIRST append failure in
            // a cycle, skip the remaining appends so we log at most one warning
            // per cycle (not one per log entry). Logs are still drained from the
            // tailer for the push path regardless of staging health.
            var staging_failed = false;

            // beginCycle takes the EX roll lock; if even that fails we can't
            // safely append this cycle - warn once and drain logs for push only.
            // `cycle_open` tracks whether the lock is held: endCycle (which
            // RELEASES the lock) MUST run whenever begin succeeded, even if an
            // append later failed, or we would leak the EX lock and wedge every
            // future cycle + any query/roll. So endCycle is gated on cycle_open,
            // NOT on staging_failed.
            var cycle_open = false;
            stg.beginCycle() catch |err| {
                std.debug.print("Warning: staging beginCycle failed: {}\n", .{err});
                staging_failed = true;
            };
            if (!staging_failed) cycle_open = true;

            if (!staging_failed) {
                stg.appendMetrics(now, metrics) catch |err| {
                    std.debug.print("Warning: metrics append failed: {}\n", .{err});
                    staging_failed = true;
                };
            }
            if (!staging_failed and procs.len > 0) {
                stg.appendProcesses(now, procs) catch |err| {
                    std.debug.print("Warning: process append failed: {}\n", .{err});
                    staging_failed = true;
                };
            }
            if (!staging_failed and disks.len > 0) {
                stg.appendDisks(now, disks) catch |err| {
                    std.debug.print("Warning: disk append failed: {}\n", .{err});
                    staging_failed = true;
                };
            }
            if (!staging_failed and containers.len > 0) {
                stg.appendContainers(now, containers) catch |err| {
                    std.debug.print("Warning: container append failed: {}\n", .{err});
                    staging_failed = true;
                };
            }
            if (!staging_failed and ct_metrics.len > 0) {
                stg.appendContainerMetrics(now, ct_metrics) catch |err| {
                    std.debug.print("Warning: container_metrics append failed: {}\n", .{err});
                    staging_failed = true;
                };
            }

            // Drain available log entries. The push path is independent of
            // staging health; only skip the staging append when staging failed
            // this cycle. Each entry is appended individually here (one record
            // per log entry mirrors the old per-entry insertLog), but they all
            // sit inside this one begin/end bracket and share one fdatasync.
            if (log_tailer) |*lt| {
                var log_count: u32 = 0;
                while (log_count < 1000) : (log_count += 1) {
                    const maybe_entry = lt.next() catch break;
                    if (maybe_entry == null) break;
                    const entry = maybe_entry.?;
                    if (!staging_failed) {
                        // appendLogs takes a slice; pass this single entry.
                        stg.appendLogs(&[_]logs_mod.LogEntry{entry}) catch |err| {
                            std.debug.print("Warning: log append failed: {}\n", .{err});
                            staging_failed = true;
                        };
                    }
                    push_logs.append(allocator, entry) catch |err| {
                        var owned_entry = entry;
                        owned_entry.deinit(allocator);
                        return err;
                    };
                }
            }

            // Close the cycle: fdatasync every touched segment THEN release the
            // EX lock (endCycle does both, the unlock via defer even if the sync
            // errors). Gated on cycle_open so the lock is ALWAYS released once
            // begin took it - a mid-cycle append failure must not strand it.
            if (cycle_open) {
                stg.endCycle() catch |err| {
                    std.debug.print("Warning: staging endCycle failed: {}\n", .{err});
                };
            }
        }

        // ── ROLL TRIGGER ──
        // After committing the cycle, roll each table whose segment has grown
        // past roll_max_bytes OR whose last roll was more than roll_interval_s
        // ago (so low-volume tables - disks, containers - still roll and don't
        // sit un-rolled indefinitely). rollTable resets the segment on success
        // and is a no-op for an empty segment. Failures are per-table, logged,
        // and non-fatal (the rows stay durably in staging for the next attempt).
        for (staging_mod.Table.all) |table| {
            const idx = @intFromEnum(table);
            const size = stg.byteLen(table) catch 0;
            const size_trigger = size > roll_max_bytes;
            const time_trigger = (now - last_roll[idx]) >= @as(i64, @intCast(roll_interval_s));
            if (!size_trigger and !time_trigger) continue;
            if (roll_mod.rollTable(allocator, root, &stg, table)) |maybe_res| {
                if (maybe_res) |res| allocator.free(res.parquet_path);
                // Reset the time trigger even when there was nothing to roll, so
                // an idle table doesn't re-evaluate the (cheap) trigger every cycle.
                last_roll[idx] = now;
            } else |err| {
                std.debug.print("Warning: roll of {s} failed: {}\n", .{ table.name(), err });
            }
        }

        // ── RETENTION + COMPACTION (hourly) ──
        // Retention drops whole stale date= partitions (best-effort, non-fatal).
        // Compaction then merges each SEALED day (a date= dir older than today/
        // UTC) into one day-level file, shrinking the per-query file count from
        // ~one-per-hour-partition to ~one-per-day. Both run under the same EX-lock
        // discipline (taken internally), so they're mutually exclusive with rolls
        // and query snapshots. Compaction runs AFTER retention so it never merges
        // a day retention is about to drop.
        retention_counter += interval;
        if (retention_counter >= 3600) {
            const retention = if (config) |c| c.value.retention orelse default_retention else default_retention;
            roll_mod.runRetention(allocator, root, retention) catch |err| {
                std.debug.print("Warning: retention cleanup failed: {}\n", .{err});
            };
            // A PRE-COMMIT compaction failure is best-effort (the day is untouched,
            // logged, retried next tick). A POST-COMMIT failure
            // (CompactionCommitIncomplete) is FATAL: a committed manifest may leave
            // rows only in a `.building` queries ignore, so we must NOT keep serving
            // short counts. Exit so systemd restarts us; startup recoverCompactions
            // then finishes the compaction from the durable manifest.
            roll_mod.compactSealedDays(allocator, root) catch |err| {
                if (err == error.CompactionCommitIncomplete) {
                    std.debug.print("FATAL: day compaction failed post-commit ({}); exiting so recovery can finish on restart\n", .{err});
                    std.process.exit(1);
                }
                std.debug.print("Warning: day compaction failed: {}\n", .{err});
            };
            retention_counter = 0;
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
                    rule_set,
                    self_sample,
                    // No resident DuckDB after the cutover: there are no
                    // consecutive insert failures to report, and "db size" is now
                    // the on-disk hot tier rather than a single DB file. Report 0
                    // for both (push.zig signature is unchanged by request); a
                    // hot-tier size metric is a follow-up slice.
                    0,
                    0,
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

    // Clean-shutdown flush: roll any staging segments still holding rows so they
    // don't sit un-rolled across a restart. Durability does NOT depend on this -
    // a crash is handled by the startup replay roll above - so it's best-effort
    // (catch + log, non-fatal). It just bounds segment size and makes the
    // shutdown intent (drain staging to parquet) explicit. Runs BEFORE deinit so
    // the roll reuses the still-open staging fd.
    {
        const flushed = roll_mod.rollAll(allocator, root, &stg) catch |err| blk: {
            std.debug.print("Warning: shutdown flush roll failed: {}\n", .{err});
            break :blk 0;
        };
        if (flushed > 0) std.debug.print("shutdown flush: rolled {d} staging segment(s)\n", .{flushed});
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
