const std = @import("std");
const collector_mod = @import("collector");
const logs_mod = @import("logs");
const rules_mod = @import("rules");
const proc_self_mod = @import("proc_self");
const proxmox_mod = @import("proxmox");
const push_mod = @import("push");
// Parquet hot tier (plan 25 cutover). The daemon WRITE path is a durable
// append-log (staging, PURE - always imported) periodically rolled to parquet
// (roll, links duckdb). The local hot tier is GATED behind -Dstore: under
// store=false roll is never built, so the import is a comptime stand-in (mirrors
// ner_pf_mod) and every roll_mod.* call sits behind a comptime-true branch.
// storage.zig is fully dropped from the daemon post-cutover: its only remaining
// use was the back-compat default_memory_limit_mb constant, now a local below.
const staging_mod = @import("staging");
const roll_mod = if (build_options.store_enabled) @import("roll") else struct {};
// Redaction now runs through the preprocessor pipeline (preprocessor +
// redact_adapter); main.zig no longer calls redact directly.
// NER adapter: `ner` is the pure interface (links nothing); `ner_pf` is the
// FFI-GGML backend that links libpf. main.zig is the ONLY place the concrete
// backend is constructed - everything downstream takes the pure `ner.Ner`.
const ner_mod = @import("ner");
const build_options = @import("build_options");
const preprocessor_mod = @import("preprocessor");
const redact_adapter_mod = @import("redact_adapter");
// ner_pf is the concrete FFI backend; it is ONLY built under -Dner. The import
// is gated on the comptime-known build_options.ner_enabled so the base build
// never analyzes @import("ner_pf") (the module does not exist in that graph).
// The empty-struct stand-in keeps the name resolvable; loadNer only touches
// ner_pf_mod.init inside a comptime-true branch, so the stand-in is never
// type-checked for an .init decl.
const ner_pf_mod = if (build_options.ner_enabled) @import("ner_pf") else struct {};

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
// RETAINED FOR BACK-COMPAT: the resident-DuckDB write path this capped is retired
// (plan 25), so this value is parsed + logged but otherwise unused. Defined as a
// local (was storage_mod.Storage.default_memory_limit_mb) so the daemon does not
// import storage.zig at all - which is what keeps duckdb unreferenced under
// -Dstore=false.
const default_memory_limit_mb: u32 = 512;
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
    // Model-backed NER redaction (free-form names / addresses the deterministic
    // scanners can't characterize). Disabled by default: when false, or when the
    // model file is absent / fails to load, the daemon redacts with the
    // deterministic scanners ONLY - never raw, never crashing.
    enable_ner: ?bool = null,
    // Path to the GGUF model used by the NER backend. Tilde-expanded. Only read
    // when enable_ner is true. Absent + enable_ner=true => degrade to scanners.
    ner_model_path: ?[]const u8 = null,
    // Ordered preprocessor chain run once per cycle before staging-append/push.
    // null (field absent) => default ["redact"] (today's always-on deterministic
    // redaction; backward compatible). [] => lightweight pure passthrough (NO
    // redaction). ["redact"] => deterministic byte-scanners only. ["redact","ner"]
    // => deterministic + model-backed (ner stage skipped with a warning if the
    // daemon was built without -Dner or the model failed to load - never crashes,
    // never emits raw PII). Mapping null -> ["redact"] (NOT []) is mandatory:
    // mapping null to [] would silently disable redaction for every existing config.
    preprocessors: ?[]const []const u8 = null,
    // Runtime FORWARD-ONLY switch for the local parquet hot tier. null (absent) =>
    // true, so every existing config.json is unchanged (storage active in a
    // store-capable build). false => skip staging append + roll + retention/
    // compaction at RUNTIME even though the modules are compiled in; previously
    // written parquet stays on disk and the CLI can still query it. This is NOT a
    // live on/off toggle for queries - it stops new local writes only. Ignored
    // (parsed-but-dead) when the daemon was built with -Dstore=false. Follows the
    // back-compat-field precedent (memory_limit_mb / storage_refresh_*).
    local_store: ?bool = null,
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

    // The effective rule set is `local ++ server-pushed`, mutable across the
    // loop's lifetime. Local first means a hand-edited log_rules.json always
    // wins over what the control plane ships. `combined_counts` carries the
    // per-rule sample counters; the local prefix is preserved when server
    // rules get refreshed, the server suffix resets to zero.
    var combined_rules = try allocator.alloc(rules_mod.LogRule, log_rules.len);
    defer allocator.free(combined_rules);
    @memcpy(combined_rules, log_rules);

    var combined_counts = try allocator.alloc(u64, log_rules.len);
    defer allocator.free(combined_counts);
    @memset(combined_counts, 0);

    var rule_set = rules_mod.RuleSet{ .rules = combined_rules, .sample_counts = combined_counts };

    // The most recent ingest response; held so server-pushed rule slices
    // stay valid for `combined_rules` to reference across cycles. Replaced
    // (old deinited) whenever a fresh response brings new rules.
    var server_rules_parsed: ?std.json.Parsed(push_mod.IngestResponse) = null;
    defer if (server_rules_parsed) |p| p.deinit();

    var db_path: []const u8 = if (config) |c| c.value.db_path orelse default_db_path else default_db_path;
    var interval: u64 = if (config) |c| c.value.interval orelse default_interval else default_interval;
    var server_url: ?[]const u8 = if (config) |c| c.value.server_url else null;
    var api_key: ?[]const u8 = if (config) |c| c.value.api_key else null;
    // RETAINED FOR BACK-COMPAT: still parsed so old config.json files load, but
    // the resident-DuckDB write path it capped is retired. Logged at startup.
    const memory_limit_mb: u32 = if (config) |c|
        c.value.memory_limit_mb orelse default_memory_limit_mb
    else
        default_memory_limit_mb;
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

    // Local hot tier gating. `local_store` is the RUNTIME knob (null => true).
    // `store_active` composes the comptime build gate with the runtime knob: a
    // store-capable build (build_options.store_enabled) with local_store=true
    // writes locally; either off => no local write this cycle. BUILD-only gating
    // (build_options.store_enabled alone) wraps startup-lifecycle / roll-replay /
    // retention / shutdown, which reference roll_mod and so must vanish under
    // -Dstore=false; RUNTIME gating (store_active) wraps the per-cycle append +
    // roll-trigger, which exist in the graph but are skipped forward-only.
    const local_store: bool = if (config) |c| c.value.local_store orelse true else true;
    const store_active = build_options.store_enabled and local_store;

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

    // The durable staging append-log. Its TYPE is comptime-conditional: a real
    // Staging only in a store-capable build (where the open + lifecycle below
    // run), otherwise `void` so the daemon never opens it and roll_mod (a stand-in
    // struct under -Dstore=false) is never referenced. Declared here so the
    // per-cycle write path (gated on store_active) and shutdown flush can see it.
    var stg: if (build_options.store_enabled) staging_mod.Staging else void = undefined;

    // STARTUP LIFECYCLE (must run BEFORE any append/roll/query). BUILD-time gated:
    // every call here touches roll_mod, which does not exist under -Dstore=false.
    if (build_options.store_enabled) {
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
        stg = try staging_mod.Staging.open(allocator, root);

        // REPLAY: re-roll any staging segments left over from a prior run so a crash
        // mid-run does not strand un-rolled rows in staging (and so the segment
        // doesn't keep growing past its size trigger across restarts). rollAll is a
        // no-op for empty segments.
        const replayed = roll_mod.rollAll(allocator, root, &stg) catch |err| blk: {
            std.debug.print("Warning: startup replay roll failed: {}\n", .{err});
            break :blk 0;
        };
        if (replayed > 0) std.debug.print("startup replay: rolled {d} leftover staging segment(s)\n", .{replayed});
    }
    defer if (build_options.store_enabled) stg.deinit();

    // Obsolete-config visibility: the resident-DuckDB refresh knobs are parsed for
    // back-compat but do nothing post-cutover. If an operator still has either set,
    // warn once so they aren't misled into thinking it's taking effect.
    if (config) |c| {
        if (c.value.storage_refresh_interval != null or c.value.storage_refresh_rss_mb != null) {
            std.log.warn("config: storage_refresh_interval / storage_refresh_rss_mb are obsolete post-cutover (no resident DuckDB to refresh) - ignored", .{});
        }
    }

    // Storage mode: on (store build + local_store=true), disabled-build (built
    // with -Dstore=false), or disabled-runtime (store build but local_store=false,
    // forward-only: no new local writes, prior parquet still queryable).
    const store_mode: []const u8 = if (!build_options.store_enabled)
        "disabled-build"
    else if (!local_store)
        "disabled-runtime (local_store=false)"
    else
        "on";
    std.debug.print("sermon-agent started (root={s}, interval={d}s, max_processes={d}, roll_max_bytes={d}, roll_interval_s={d}s, local_store={s}) [memory_limit_mb={d} retained for back-compat, unused]\n", .{ root, interval, max_processes, roll_max_bytes, roll_interval_s, store_mode, memory_limit_mb });
    // Warn once if store knobs are set on a build that can't honor them.
    if (!build_options.store_enabled) {
        if (config) |c| {
            if (c.value.local_store != null or c.value.roll_max_bytes != null or c.value.roll_interval_s != null) {
                std.log.warn("config: local_store / roll_max_bytes / roll_interval_s are ignored on a -Dstore=false build (no local hot tier)", .{});
            }
        }
    }
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

    // ── NER backend (model-backed PII redaction) ──
    // Loaded ONCE here (the model is GBs resident; load-per-call is untenable).
    // null = deterministic-scanners-only; loadNer never crashes and never runs
    // raw. The handle is threaded into redactProcesses / redactLog below.
    const ner_backend: ?ner_mod.Ner = loadNer(allocator, config);
    // Build the preprocessor chain once. buildPipeline takes OWNERSHIP of
    // ner_backend (frees it if no "ner" stage uses it, or via the stage's deinit
    // otherwise), so there is NO separate ner_backend deinit here - that would
    // double-free. Default chain (config preprocessors absent) is ["redact"].
    const cfg_value: Config = if (config) |c| c.value else .{};
    const pipeline = try buildPipeline(allocator, cfg_value, ner_backend);
    defer pipeline.deinit(allocator);

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
    // begins. Non-fatal: best-effort, log once and continue. BUILD-gated: touches
    // roll_mod. Retention runs whenever the build is store-capable (independent of
    // the runtime local_store knob) so a build that previously wrote parquet still
    // trims it even when forward-only writes are paused.
    if (build_options.store_enabled) {
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

        // ── EDGE PII REDACTION (single chokepoint) ──
        // Redact the in-memory ProcessInfo / DiskInfo slices IN PLACE, here,
        // AFTER collection and BEFORE both the staging append (local parquet)
        // and buildPayload (remote upload), so both paths see identical
        // redacted bytes. Each helper frees the old owned string field and
        // swaps in a fresh redacted one (the defers above still free the
        // current pointers). Log entries are redacted individually as they are
        // drained, below. A redaction failure is fatal-for-cycle: we must never
        // persist or push raw PII, so we skip this cycle rather than fall
        // through with unredacted data.
        pipeline.runBatch(allocator, procs, disks) catch |err| {
            std.debug.print("Warning: process/disk redaction failed: {}\n", .{err});
            continue;
        };

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
        //
        // GATING: the staging append is BUILD-gated (comptime build_options
        // .store_enabled, so under -Dstore=false the `void` stg is never touched)
        // AND RUNTIME-gated (store_active: a store-capable build with
        // local_store=false skips the append forward-only). `store_active` already
        // implies build_options.store_enabled. The log-drain loop MUST still run
        // when storage is off because it feeds push_logs (remote push); only the
        // per-entry stg.appendLogs call is gated.
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
            if (build_options.store_enabled and store_active) {
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
            }

            // Drain available log entries. The push path is independent of
            // staging health AND of the store gate; only skip the staging append
            // when staging failed this cycle or local storage is off. Each entry
            // is appended individually here (one record per log entry mirrors the
            // old per-entry insertLog), but they all sit inside this one begin/end
            // bracket and share one fdatasync.
            if (log_tailer) |*lt| {
                var log_count: u32 = 0;
                while (log_count < 1000) : (log_count += 1) {
                    const maybe_entry = lt.next() catch break;
                    if (maybe_entry == null) break;
                    var entry = maybe_entry.?;
                    // Redact this log entry IN PLACE before it touches either
                    // the staging append or the push buffer. On the rare OOM
                    // failure, drop the entry (free it, skip it) rather than
                    // persist/push raw PII - one dropped log line is acceptable;
                    // leaking a credential is not.
                    pipeline.runLog(allocator, &entry) catch |err| {
                        std.debug.print("Warning: log redaction failed, dropping entry: {}\n", .{err});
                        entry.deinit(allocator);
                        continue;
                    };
                    if (build_options.store_enabled and store_active and !staging_failed) {
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
            if (build_options.store_enabled and cycle_open) {
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
        // RUNTIME-gated on store_active (references stg + roll_mod): a store build
        // with local_store=false stops rolling new segments forward-only.
        if (build_options.store_enabled and store_active) {
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
        }

        // ── RETENTION + COMPACTION (hourly) ──
        // Retention drops whole stale date= partitions (best-effort, non-fatal).
        // Compaction then merges each SEALED day (a date= dir older than today/
        // UTC) into one day-level file, shrinking the per-query file count from
        // ~one-per-hour-partition to ~one-per-day. Both run under the same EX-lock
        // discipline (taken internally), so they're mutually exclusive with rolls
        // and query snapshots. Compaction runs AFTER retention so it never merges
        // a day retention is about to drop.
        // BUILD-gated (touches roll_mod): retention/compaction maintain previously
        // written parquet, so they run on any store-capable build regardless of the
        // runtime local_store knob.
        retention_counter += interval;
        if (build_options.store_enabled and retention_counter >= 3600) {
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
                    const maybe_response = push_mod.pushMetrics(allocator, url, key, payload) catch |err| push_blk: {
                        std.debug.print("Warning: metrics push failed: {}\n", .{err});
                        break :push_blk null;
                    };

                    if (maybe_response) |response| {
                        // Rebuild the effective rule set from local + server-
                        // pushed. On alloc failure, keep the previous rule_set
                        // and discard the new response.
                        if (rules_mod.combineRules(allocator, log_rules, response.value.log_rules, combined_counts)) |combined| {
                            allocator.free(combined_rules);
                            allocator.free(combined_counts);
                            combined_rules = combined.rules;
                            combined_counts = combined.counts;
                            rule_set = .{ .rules = combined_rules, .sample_counts = combined_counts };
                            if (server_rules_parsed) |old| old.deinit();
                            server_rules_parsed = response;
                        } else |err| {
                            std.debug.print("Warning: rule set update failed: {}\n", .{err});
                            response.deinit();
                        }
                    }
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
    // the roll reuses the still-open staging fd. BUILD-gated (touches roll_mod +
    // the void stg under -Dstore=false). rollAll is a no-op for empty segments, so
    // it is safe to flush even when local_store=false paused new appends.
    if (build_options.store_enabled) {
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

/// Load the model-backed NER redaction backend ONCE at daemon start, with full
/// degrade-to-deterministic semantics: returns null (scanners-only) when NER is
/// disabled, no model path is configured, the path can't be expanded, the file
/// is absent, or pf_load fails. NEVER crashes and NEVER runs raw - a null here
/// just means the deterministic scanners do all the work. The returned Ner owns
/// the loaded model for the daemon lifetime; the caller must `deinit` it.
///
/// Thread-safety: the returned handle is NOT safe for concurrent classify (the
/// pf_ctx gallocr buffer is reused without locks). The daemon redacts single-
/// threaded at one chokepoint, so holding one handle is correct today.
/// Build the preprocessor chain from config. Takes FULL OWNERSHIP of
/// `ner_backend`: if an "ner" stage is created it owns the backend (freed via
/// the stage's deinit in Pipeline.deinit); if no "ner" stage is created but a
/// backend was loaded, this fn frees it before returning. The caller therefore
/// must NOT separately deinit ner_backend (avoids the double-free risk).
///
/// names == null (config field absent) => default ["redact"]. names == [] =>
/// pure passthrough. Unknown names and an "ner" stage with no usable backend are
/// warned-and-skipped: degrade to whatever stages remain, never crash, never
/// emit raw PII.
fn buildPipeline(
    allocator: std.mem.Allocator,
    cfg: Config,
    ner_backend: ?ner_mod.Ner,
) ner_mod.Error!preprocessor_mod.Pipeline {
    const default_chain = [_][]const u8{"redact"};
    const names: []const []const u8 = cfg.preprocessors orelse &default_chain;

    var stages = std.ArrayList(preprocessor_mod.Preprocessor){};
    errdefer stages.deinit(allocator);

    var ner_consumed = false;
    for (names) |name| {
        if (std.mem.eql(u8, name, "redact")) {
            try stages.append(allocator, redact_adapter_mod.DeterministicRedact.asPreprocessor());
        } else if (std.mem.eql(u8, name, "ner")) {
            if (!build_options.ner_enabled or ner_backend == null) {
                std.debug.print("config: preprocessor \"ner\" requested but unavailable (built without -Dner or model not loaded) - skipping, deterministic stages only\n", .{});
                continue;
            }
            const nr = try allocator.create(redact_adapter_mod.NerRedact);
            nr.* = .{ .ner_backend = ner_backend.? };
            try stages.append(allocator, nr.asPreprocessor());
            ner_consumed = true;
        } else {
            std.debug.print("config: unknown preprocessor \"{s}\" - skipping\n", .{name});
        }
    }

    // We own ner_backend. If no ner stage consumed it, free it now.
    if (!ner_consumed) {
        if (ner_backend) |n| n.deinit();
    }

    return .{ .stages = try stages.toOwnedSlice(allocator) };
}

fn loadNer(allocator: std.mem.Allocator, config: ?std.json.Parsed(Config)) ?ner_mod.Ner {
    // Comptime gate: in the base build (no -Dner) ner_pf_mod is an empty struct,
    // so the rest of this fn (ner_pf_mod.init) must be dead code Zig never
    // analyzes. ner_enabled is comptime-known, so the early return elides it.
    if (!build_options.ner_enabled) return null;
    const cfg = (config orelse return null).value;
    if (!(cfg.enable_ner orelse false)) return null;
    const raw_path = cfg.ner_model_path orelse {
        std.debug.print("config: enable_ner=true but no ner_model_path set - NER disabled, deterministic scanners only\n", .{});
        return null;
    };
    const expanded = expandPath(allocator, raw_path) catch return null;
    defer allocator.free(expanded);
    // pf_load needs a NUL-terminated C string.
    const path_z = allocator.dupeZ(u8, expanded) catch return null;
    defer allocator.free(path_z);
    // Fast pre-check so a missing model logs a clear message instead of relying
    // on pf_load's internal error path.
    std.fs.cwd().access(path_z, .{}) catch {
        std.debug.print("config: ner_model_path {s} not found - NER disabled, deterministic scanners only\n", .{path_z});
        return null;
    };
    const n = ner_pf_mod.init(allocator, path_z) catch |err| {
        std.debug.print("Warning: NER model load failed ({}) - deterministic scanners only\n", .{err});
        return null;
    };
    std.debug.print("NER model loaded from {s} (model-backed PII redaction active)\n", .{path_z});
    return n;
}
