const std = @import("std");
const build_options = @import("build_options");
const collector_mod = @import("collector");
const logs_mod = @import("logs");

const Allocator = std.mem.Allocator;

const max_logs_per_payload = 100;
const max_log_message_bytes = 4_096;

const Payload = struct {
    hostname: []const u8,
    daemon_version: []const u8,
    collected_at: i64,
    metrics: MetricsPayload,
    processes: []const ProcessPayload,
    disks: []const DiskPayload,
    logs: []const LogPayload,
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

    const log_count = @min(log_entries.len, max_logs_per_payload);
    const log_start = log_entries.len - log_count;
    const payload_logs = try allocator.alloc(LogPayload, log_count);
    defer allocator.free(payload_logs);

    for (payload_logs, log_entries[log_start..]) |*dest, src| {
        dest.* = .{
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
        .logs = payload_logs,
    };

    return std.fmt.allocPrint(allocator, "{f}", .{std.json.fmt(payload, .{})});
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

    const payload = try buildPayload(allocator, "host-a", 1_739_443_200, metrics, &procs, &disks, &.{});
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

    const payload = try buildPayload(allocator, "host-a", 1_739_443_200, metrics, &procs, &disks, &entries);
    defer allocator.free(payload);

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, payload, .{});
    defer parsed.deinit();

    const root = parsed.value.object;
    const log_values = root.get("logs").?.array;

    try std.testing.expectEqual(@as(usize, 100), log_values.items.len);
    try std.testing.expectEqual(@as(i64, 1_739_443_001), log_values.items[0].object.get("timestamp").?.integer);
    try std.testing.expectEqualStrings("sshd", log_values.items[0].object.get("unit").?.string);
    try std.testing.expectEqualStrings("sshd", log_values.items[0].object.get("identifier").?.string);
    try std.testing.expectEqualStrings("ssh.service", log_values.items[0].object.get("systemd_unit").?.string);
    try std.testing.expectEqual(@as(usize, 4_096), log_values.items[0].object.get("message").?.string.len);
}

test "buildIngestUrl trims trailing slash" {
    const allocator = std.testing.allocator;

    const url = try buildIngestUrl(allocator, "http://localhost:4000/");
    defer allocator.free(url);

    try std.testing.expectEqualStrings("http://localhost:4000/api/ingest", url);
}
