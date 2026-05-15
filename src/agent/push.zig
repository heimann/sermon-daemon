const std = @import("std");
const build_options = @import("build_options");
const collector_mod = @import("collector");
const logs_mod = @import("logs");
const proc_self_mod = @import("proc_self");
const proxmox_mod = @import("proxmox");

const Allocator = std.mem.Allocator;

const max_logs_per_payload = 20;
const max_log_message_bytes = 4_096;

const Payload = struct {
    hostname: []const u8,
    daemon_version: []const u8,
    collected_at: i64,
    metrics: MetricsPayload,
    processes: []const ProcessPayload,
    disks: []const DiskPayload,
    logs: []const LogPayload,
    log_stats: LogStatsPayload,
    daemon_self: DaemonSelfPayload,
    runtime: RuntimePayload,
    containers: []const ContainerPayload,
    container_metrics: []const ContainerMetricPayload,
};

// Runtime context for the host emitting this payload. `kind` discriminates
// the rest of the fields. Hosted side can branch on `kind` to decide whether
// to render Proxmox-specific UI (host badge, containers panel) or treat the
// host as a plain Linux box. New `kind` values can be added without breaking
// older hosted versions; they'll just render as plain Linux until the hosted
// schema catches up.
const RuntimePayload = struct {
    kind: []const u8, // "not_proxmox" | "proxmox_host" | "proxmox_container"
    node: ?[]const u8 = null, // proxmox_host: cluster node name
    version: ?[]const u8 = null, // proxmox_host: pveversion line
    vmid: ?u32 = null, // proxmox_container: container's vmid in the host's PVE
};

const ContainerPayload = struct {
    vmid: u32,
    name: []const u8,
    node: []const u8,
    type: []const u8, // "lxc" | "qemu"
    status: []const u8, // "running" | "stopped" | "paused" | ...
    maxmem: u64,
    maxcpu: f64,
    uptime: u64,
};

// Per-container point-in-time metrics. cpu_pct is null for the first cycle
// after a CT appears (no prior delta); mem_max is null when the CT has no
// memory limit (cgroup memory.max == "max"). Hosted side renders nulls as
// "no data yet" / "unlimited" rather than 0.
const ContainerMetricPayload = struct {
    vmid: u32,
    cpu_pct: ?f64,
    mem_current: u64,
    mem_max: ?u64,
};

const DaemonSelfPayload = struct {
    cpu_percent: f32,
    rss_kb: u64,
    vsize_kb: u64,
    threads: u32,
    voluntary_ctxt_switches: u64,
    nonvoluntary_ctxt_switches: u64,
    uptime_seconds: u64,
    consecutive_insert_failures: u32,
    db_size_bytes: u64,
};

const MetricsPayload = struct {
    cpu_percent: f32,
    cpu_user: f32,
    cpu_system: f32,
    cpu_iowait: f32,
    mem_total: u64,
    mem_used: u64,
    mem_percent: f32,
    swap_total: u64,
    swap_used: u64,
};

const ProcessPayload = struct {
    pid: u32,
    name: []const u8,
    state: []const u8,
    cpu_percent: f32,
    mem_rss: u64,
    threads: u32,
    username: []const u8,
};

const DiskPayload = struct {
    mount_point: []const u8,
    filesystem: []const u8,
    total_bytes: u64,
    used_bytes: u64,
    percent: f32,
};

const LogStatsPayload = struct {
    seen: usize,
    uploaded: usize,
    dropped: usize,
};

const LogPayload = struct {
    timestamp: i64,
    source: []const u8,
    unit: ?[]const u8,
    identifier: ?[]const u8,
    systemd_unit: ?[]const u8,
    priority: u8,
    pid: ?u32,
    message: []const u8,
};

pub fn buildPayload(
    allocator: Allocator,
    hostname: []const u8,
    timestamp: i64,
    metrics: collector_mod.SystemMetrics,
    procs: []const collector_mod.ProcessInfo,
    disks: []const collector_mod.DiskInfo,
    log_entries: []const logs_mod.LogEntry,
    daemon_self: proc_self_mod.Sample,
    consecutive_insert_failures: u32,
    db_size_bytes: u64,
    runtime: proxmox_mod.Runtime,
    containers: []const proxmox_mod.ContainerEntry,
    container_metrics: []const proxmox_mod.ContainerMetrics,
) ![]u8 {
    var sorted_procs = try allocator.dupe(collector_mod.ProcessInfo, procs);
    defer allocator.free(sorted_procs);

    std.mem.sort(collector_mod.ProcessInfo, sorted_procs, {}, struct {
        fn lessThan(_: void, a: collector_mod.ProcessInfo, b: collector_mod.ProcessInfo) bool {
            return a.cpu_percent > b.cpu_percent;
        }
    }.lessThan);

    const proc_count = @min(sorted_procs.len, 25);
    const payload_procs = try allocator.alloc(ProcessPayload, proc_count);
    defer allocator.free(payload_procs);

    const state_buffers = try allocator.alloc([1]u8, proc_count);
    defer allocator.free(state_buffers);

    for (payload_procs, sorted_procs[0..proc_count], 0..) |*dest, src, idx| {
        state_buffers[idx][0] = src.state;
        dest.* = .{
            .pid = src.pid,
            .name = src.name,
            .state = state_buffers[idx][0..1],
            .cpu_percent = src.cpu_percent,
            .mem_rss = src.mem_rss,
            .threads = src.threads,
            .username = src.username,
        };
    }

    const payload_disks = try allocator.alloc(DiskPayload, disks.len);
    defer allocator.free(payload_disks);

    for (payload_disks, disks) |*dest, src| {
        dest.* = .{
            .mount_point = src.mount_point,
            .filesystem = src.filesystem,
            .total_bytes = src.total_bytes,
            .used_bytes = src.used_bytes,
            .percent = src.percent,
        };
    }

    const max_log_count = @min(log_entries.len, max_logs_per_payload);
    const payload_logs = try allocator.alloc(LogPayload, max_log_count);
    defer allocator.free(payload_logs);

    const uploaded_logs = selectLogsForUpload(payload_logs, log_entries);
    const dropped_logs = log_entries.len - uploaded_logs.len;

    const runtime_payload: RuntimePayload = switch (runtime) {
        .not_proxmox => .{ .kind = "not_proxmox" },
        .host => |h| .{ .kind = "proxmox_host", .node = h.node, .version = h.runtime_version },
        .container => |ct| .{ .kind = "proxmox_container", .vmid = ct.vmid },
    };

    const payload_containers = try allocator.alloc(ContainerPayload, containers.len);
    defer allocator.free(payload_containers);

    for (payload_containers, containers) |*dest, src| {
        dest.* = .{
            .vmid = src.vmid,
            .name = src.name,
            .node = src.node,
            .type = src.type,
            .status = src.status,
            .maxmem = src.maxmem,
            .maxcpu = src.maxcpu,
            .uptime = src.uptime,
        };
    }

    const payload_ct_metrics = try allocator.alloc(ContainerMetricPayload, container_metrics.len);
    defer allocator.free(payload_ct_metrics);

    for (payload_ct_metrics, container_metrics) |*dest, src| {
        dest.* = .{
            .vmid = src.vmid,
            .cpu_pct = if (std.math.isNan(src.cpu_pct)) null else src.cpu_pct,
            .mem_current = src.mem_current,
            .mem_max = src.mem_max,
        };
    }

    const payload = Payload{
        .hostname = hostname,
        .daemon_version = build_options.version,
        .collected_at = timestamp,
        .metrics = .{
            .cpu_percent = metrics.cpu_percent,
            .cpu_user = metrics.cpu_user,
            .cpu_system = metrics.cpu_system,
            .cpu_iowait = metrics.cpu_iowait,
            .mem_total = metrics.mem_total,
            .mem_used = metrics.mem_used,
            .mem_percent = metrics.mem_percent,
            .swap_total = metrics.swap_total,
            .swap_used = metrics.swap_used,
        },
        .processes = payload_procs,
        .disks = payload_disks,
        .logs = uploaded_logs,
        .log_stats = .{
            .seen = log_entries.len,
            .uploaded = uploaded_logs.len,
            .dropped = dropped_logs,
        },
        .daemon_self = .{
            .cpu_percent = daemon_self.cpu_percent,
            .rss_kb = daemon_self.rss_kb,
            .vsize_kb = daemon_self.vsize_kb,
            .threads = daemon_self.threads,
            .voluntary_ctxt_switches = daemon_self.voluntary_ctxt_switches,
            .nonvoluntary_ctxt_switches = daemon_self.nonvoluntary_ctxt_switches,
            .uptime_seconds = daemon_self.uptime_seconds,
            .consecutive_insert_failures = consecutive_insert_failures,
            .db_size_bytes = db_size_bytes,
        },
        .runtime = runtime_payload,
        .containers = payload_containers,
        .container_metrics = payload_ct_metrics,
    };

    return std.fmt.allocPrint(allocator, "{f}", .{std.json.fmt(payload, .{})});
}

fn selectLogsForUpload(dest: []LogPayload, entries: []const logs_mod.LogEntry) []LogPayload {
    var count: usize = 0;

    var idx = entries.len;
    while (idx > 0 and count < dest.len) {
        idx -= 1;
        if (entries[idx].priority <= 4) {
            dest[count] = logPayload(entries[idx]);
            count += 1;
        }
    }

    idx = entries.len;
    while (idx > 0 and count < dest.len) {
        idx -= 1;
        if (entries[idx].priority > 4) {
            dest[count] = logPayload(entries[idx]);
            count += 1;
        }
    }

    return dest[0..count];
}

fn logPayload(src: logs_mod.LogEntry) LogPayload {
    return .{
        .timestamp = src.timestamp,
        .source = src.source,
        .unit = src.unit,
        .identifier = src.identifier,
        .systemd_unit = src.systemd_unit,
        .priority = src.priority,
        .pid = src.pid,
        .message = truncateUtf8(src.message, max_log_message_bytes),
    };
}

fn truncateUtf8(value: []const u8, max_bytes: usize) []const u8 {
    if (value.len <= max_bytes) return value;

    var end = max_bytes;
    while (end > 0) : (end -= 1) {
        const candidate = value[0..end];
        if (std.unicode.utf8ValidateSlice(candidate)) return candidate;
    }

    return value[0..0];
}

pub fn pushMetrics(
    allocator: Allocator,
    server_url: []const u8,
    api_key: []const u8,
    payload: []const u8,
) !void {
    const ingest_url = try buildIngestUrl(allocator, server_url);
    defer allocator.free(ingest_url);

    var client = std.http.Client{ .allocator = allocator };
    defer client.deinit();

    const result = try client.fetch(.{
        .location = .{ .url = ingest_url },
        .method = .POST,
        .payload = payload,
        .extra_headers = &.{
            .{ .name = "content-type", .value = "application/json" },
            .{ .name = "x-sermon-ingestion-key", .value = api_key },
        },
    });

    if (result.status.class() != .success) {
        return error.IngestRejected;
    }
}

fn buildIngestUrl(allocator: Allocator, server_url: []const u8) ![]u8 {
    const trimmed = std.mem.trimRight(u8, server_url, "/");
    return std.fmt.allocPrint(allocator, "{s}/api/ingest", .{trimmed});
}

fn testSelfSample() proc_self_mod.Sample {
    return .{
        .cpu_percent = 0.42,
        .rss_kb = 38_000,
        .vsize_kb = 311_000,
        .threads = 1,
        .voluntary_ctxt_switches = 100,
        .nonvoluntary_ctxt_switches = 10,
        .uptime_seconds = 60,
    };
}

test "buildPayload caps processes, sorts by CPU, and omits cmdline" {
    const allocator = std.testing.allocator;

    const metrics = collector_mod.SystemMetrics{
        .cpu_percent = 55.0,
        .cpu_user = 40.0,
        .cpu_system = 15.0,
        .cpu_iowait = 0.0,
        .mem_total = 16_000,
        .mem_used = 8_000,
        .mem_percent = 50.0,
        .swap_total = 2_000,
        .swap_used = 100,
    };

    var procs: [30]collector_mod.ProcessInfo = undefined;
    for (&procs, 0..) |*proc, i| {
        const n = i + 1;
        proc.* = .{
            .pid = @intCast(n),
            .name = "proc",
            .cmdline = "secret-token=abc123",
            .state = 'R',
            .cpu_percent = @floatFromInt(n),
            .mem_rss = 1_024,
            .threads = 1,
            .username = "root",
        };
    }

    const disks = [_]collector_mod.DiskInfo{
        .{
            .mount_point = "/",
            .filesystem = "/dev/sda1",
            .total_bytes = 100,
            .used_bytes = 50,
            .percent = 50.0,
        },
    };

    const payload = try buildPayload(allocator, "host-a", 1_739_443_200, metrics, &procs, &disks, &.{}, testSelfSample(), 0, 0, .not_proxmox, &.{}, &.{});
    defer allocator.free(payload);

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, payload, .{});
    defer parsed.deinit();

    const root = parsed.value.object;
    const processes = root.get("processes").?.array;

    try std.testing.expectEqualStrings("dev", root.get("daemon_version").?.string);
    try std.testing.expectEqual(@as(usize, 25), processes.items.len);
    try std.testing.expectEqual(@as(i64, 30), processes.items[0].object.get("pid").?.integer);
    try std.testing.expectEqual(@as(i64, 6), processes.items[24].object.get("pid").?.integer);
    try std.testing.expect(processes.items[0].object.get("cmdline") == null);

    const daemon_self = root.get("daemon_self").?.object;
    try std.testing.expectEqual(@as(i64, 38_000), daemon_self.get("rss_kb").?.integer);
    try std.testing.expectEqual(@as(i64, 1), daemon_self.get("threads").?.integer);
    try std.testing.expectEqual(@as(i64, 60), daemon_self.get("uptime_seconds").?.integer);
    try std.testing.expect(daemon_self.get("cpu_percent") != null);
}

test "buildPayload exposes consecutive_insert_failures and db_size_bytes" {
    const allocator = std.testing.allocator;

    const metrics = collector_mod.SystemMetrics{
        .cpu_percent = 1.0,
        .cpu_user = 1.0,
        .cpu_system = 0.0,
        .cpu_iowait = 0.0,
        .mem_total = 16_000,
        .mem_used = 8_000,
        .mem_percent = 50.0,
        .swap_total = 2_000,
        .swap_used = 100,
    };
    const procs = [_]collector_mod.ProcessInfo{};
    const disks = [_]collector_mod.DiskInfo{};

    const payload = try buildPayload(
        allocator,
        "host-a",
        1_739_443_200,
        metrics,
        &procs,
        &disks,
        &.{},
        testSelfSample(),
        7,
        1_234_567,
        .not_proxmox,
        &.{},
        &.{},
    );
    defer allocator.free(payload);

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, payload, .{});
    defer parsed.deinit();

    const daemon_self = parsed.value.object.get("daemon_self").?.object;
    try std.testing.expectEqual(@as(i64, 7), daemon_self.get("consecutive_insert_failures").?.integer);
    try std.testing.expectEqual(@as(i64, 1_234_567), daemon_self.get("db_size_bytes").?.integer);
}

test "buildPayload includes capped truncated logs" {
    const allocator = std.testing.allocator;

    const metrics = collector_mod.SystemMetrics{
        .cpu_percent = 1.0,
        .cpu_user = 1.0,
        .cpu_system = 0.0,
        .cpu_iowait = 0.0,
        .mem_total = 16_000,
        .mem_used = 8_000,
        .mem_percent = 50.0,
        .swap_total = 2_000,
        .swap_used = 100,
    };
    const procs = [_]collector_mod.ProcessInfo{};
    const disks = [_]collector_mod.DiskInfo{};

    const long_message = try allocator.alloc(u8, 5_000);
    defer allocator.free(long_message);
    @memset(long_message, 'x');

    var entries: [101]logs_mod.LogEntry = undefined;
    for (&entries, 0..) |*entry, i| {
        entry.* = .{
            .timestamp = 1_739_443_000 + @as(i64, @intCast(i)),
            .source = "systemd",
            .unit = "sshd",
            .identifier = "sshd",
            .systemd_unit = "ssh.service",
            .priority = 4,
            .message = long_message,
            .pid = 123,
        };
    }

    const payload = try buildPayload(allocator, "host-a", 1_739_443_200, metrics, &procs, &disks, &entries, testSelfSample(), 0, 0, .not_proxmox, &.{}, &.{});
    defer allocator.free(payload);

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, payload, .{});
    defer parsed.deinit();

    const root = parsed.value.object;
    const log_values = root.get("logs").?.array;

    try std.testing.expectEqual(@as(usize, 20), log_values.items.len);
    try std.testing.expectEqual(@as(i64, 1_739_443_100), log_values.items[0].object.get("timestamp").?.integer);
    try std.testing.expectEqualStrings("sshd", log_values.items[0].object.get("unit").?.string);
    try std.testing.expectEqualStrings("sshd", log_values.items[0].object.get("identifier").?.string);
    try std.testing.expectEqualStrings("ssh.service", log_values.items[0].object.get("systemd_unit").?.string);
    try std.testing.expectEqual(@as(usize, 4_096), log_values.items[0].object.get("message").?.string.len);
    try std.testing.expectEqual(@as(i64, 101), root.get("log_stats").?.object.get("seen").?.integer);
    try std.testing.expectEqual(@as(i64, 20), root.get("log_stats").?.object.get("uploaded").?.integer);
    try std.testing.expectEqual(@as(i64, 81), root.get("log_stats").?.object.get("dropped").?.integer);
}

test "buildPayload emits proxmox_host runtime block + containers array" {
    const allocator = std.testing.allocator;

    const metrics = collector_mod.SystemMetrics{
        .cpu_percent = 1.0,
        .cpu_user = 1.0,
        .cpu_system = 0.0,
        .cpu_iowait = 0.0,
        .mem_total = 16_000,
        .mem_used = 8_000,
        .mem_percent = 50.0,
        .swap_total = 2_000,
        .swap_used = 100,
    };
    const procs = [_]collector_mod.ProcessInfo{};
    const disks = [_]collector_mod.DiskInfo{};

    const runtime = proxmox_mod.Runtime{
        .host = .{ .node = "aniara", .runtime_version = "pve-manager/9.1.1/abc" },
    };
    const containers = [_]proxmox_mod.ContainerEntry{
        .{
            .vmid = 100,
            .name = "docker",
            .node = "aniara",
            .type = "lxc",
            .status = "running",
            .maxmem = 8_589_934_592,
            .maxcpu = 4.0,
            .uptime = 11_335_313,
        },
        .{
            .vmid = 103,
            .name = "clawdbot",
            .node = "aniara",
            .type = "lxc",
            .status = "stopped",
            .maxmem = 2_147_483_648,
            .maxcpu = 2.0,
            .uptime = 0,
        },
    };

    // Per-CT metrics: one running CT with both fields, one with NaN cpu_pct
    // (first cycle, no prior delta) to verify the null serialization path.
    const ct_metrics = [_]proxmox_mod.ContainerMetrics{
        .{ .vmid = 100, .cpu_pct = 12.5, .mem_current = 2_500_000_000, .mem_max = 8_589_934_592 },
        .{ .vmid = 101, .cpu_pct = std.math.nan(f64), .mem_current = 100_000_000, .mem_max = null },
    };

    const payload = try buildPayload(
        allocator,
        "aniara",
        1_739_443_200,
        metrics,
        &procs,
        &disks,
        &.{},
        testSelfSample(),
        0,
        0,
        runtime,
        &containers,
        &ct_metrics,
    );
    defer allocator.free(payload);

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, payload, .{});
    defer parsed.deinit();

    const root = parsed.value.object;

    const runtime_obj = root.get("runtime").?.object;
    try std.testing.expectEqualStrings("proxmox_host", runtime_obj.get("kind").?.string);
    try std.testing.expectEqualStrings("aniara", runtime_obj.get("node").?.string);
    try std.testing.expectEqualStrings("pve-manager/9.1.1/abc", runtime_obj.get("version").?.string);

    const ct_array = root.get("containers").?.array;
    try std.testing.expectEqual(@as(usize, 2), ct_array.items.len);
    const first = ct_array.items[0].object;
    try std.testing.expectEqual(@as(i64, 100), first.get("vmid").?.integer);
    try std.testing.expectEqualStrings("docker", first.get("name").?.string);
    try std.testing.expectEqualStrings("lxc", first.get("type").?.string);
    try std.testing.expectEqualStrings("running", first.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 8_589_934_592), first.get("maxmem").?.integer);

    const ctm_array = root.get("container_metrics").?.array;
    try std.testing.expectEqual(@as(usize, 2), ctm_array.items.len);
    const m_first = ctm_array.items[0].object;
    try std.testing.expectEqual(@as(i64, 100), m_first.get("vmid").?.integer);
    try std.testing.expectEqual(@as(f64, 12.5), m_first.get("cpu_pct").?.float);
    try std.testing.expectEqual(@as(i64, 2_500_000_000), m_first.get("mem_current").?.integer);
    const m_second = ctm_array.items[1].object;
    try std.testing.expectEqual(std.json.Value.null, m_second.get("cpu_pct").?);
    try std.testing.expectEqual(std.json.Value.null, m_second.get("mem_max").?);
}

test "buildIngestUrl trims trailing slash" {
    const allocator = std.testing.allocator;

    const url = try buildIngestUrl(allocator, "http://localhost:4000/");
    defer allocator.free(url);

    try std.testing.expectEqualStrings("http://localhost:4000/api/ingest", url);
}
