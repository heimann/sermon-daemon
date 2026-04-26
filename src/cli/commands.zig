const std = @import("std");
const output = @import("output.zig");
const storage_mod = @import("storage");
const Allocator = std.mem.Allocator;

pub const Storage = storage_mod.Storage;
pub const QueryResult = storage_mod.QueryResult;

// Command: sermon status
pub fn cmdStatus(
    allocator: Allocator,
    storage: *Storage,
    format: output.OutputFormat,
) !void {
    const f: std.fs.File = .{ .handle = std.posix.STDOUT_FILENO };
    const stdout = f.deprecatedWriter();

    // Get latest metrics
    const metrics = try storage.getLatestMetrics();

    // Get disk usage
    const disks = try storage.getDisks(null);
    defer {
        for (disks) |d| {
            allocator.free(d.mount_point);
            allocator.free(d.filesystem);
        }
        if (disks.len > 0) allocator.free(disks);
    }

    // Get top processes by CPU
    var all_procs = try storage.getProcesses(null);
    defer {
        for (all_procs) |p| {
            allocator.free(p.name);
            allocator.free(p.cmdline);
            allocator.free(p.username);
        }
        if (all_procs.len > 0) allocator.free(all_procs);
    }

    // Sort by CPU and take top 5
    std.mem.sort(output.ProcessInfo, all_procs, {}, struct {
        fn lessThan(_: void, a: output.ProcessInfo, b: output.ProcessInfo) bool {
            return a.cpu_percent > b.cpu_percent;
        }
    }.lessThan);

    const top_procs = if (all_procs.len > 5) all_procs[0..5] else all_procs;

    // Get hostname
    var hostname_buf: [std.posix.HOST_NAME_MAX]u8 = undefined;
    const hostname = std.posix.gethostname(&hostname_buf) catch "unknown";

    try output.printStatus(
        stdout,
        format,
        allocator,
        hostname,
        metrics,
        disks,
        top_procs,
    );
}

// Command: sermon metrics [--period <duration>]
pub fn cmdMetrics(
    allocator: Allocator,
    storage: *Storage,
    format: output.OutputFormat,
    period_seconds: i64,
) !void {
    const f: std.fs.File = .{ .handle = std.posix.STDOUT_FILENO };
    const stdout = f.deprecatedWriter();

    const now = std.time.timestamp();
    const since = now - period_seconds;

    const metrics = try storage.getMetricsRange(since, now);
    defer if (metrics.len > 0) allocator.free(metrics);

    if (metrics.len == 0) {
        try stdout.print("No metrics available for the specified period\n", .{});
        return;
    }

    try output.printMetrics(stdout, format, allocator, metrics);
}

// Sort order for processes
pub const ProcessSort = enum {
    cpu,
    mem,
    name,
};

// Command: sermon processes [--sort <cpu|mem>] [--filter <pattern>]
pub fn cmdProcesses(
    allocator: Allocator,
    storage: *Storage,
    format: output.OutputFormat,
    sort_by: ProcessSort,
    filter_pattern: ?[]const u8,
) !void {
    const f: std.fs.File = .{ .handle = std.posix.STDOUT_FILENO };
    const stdout = f.deprecatedWriter();

    const all_procs = try storage.getProcesses(null);
    defer {
        for (all_procs) |p| {
            allocator.free(p.name);
            allocator.free(p.cmdline);
            allocator.free(p.username);
        }
        if (all_procs.len > 0) allocator.free(all_procs);
    }

    // Filter if pattern is provided
    var filtered_procs: []output.ProcessInfo = @constCast(all_procs);
    if (filter_pattern) |pattern| {
        var filtered_list = std.ArrayList(output.ProcessInfo){};
        defer filtered_list.deinit(allocator);

        for (all_procs) |proc| {
            if (std.mem.indexOf(u8, proc.name, pattern) != null or
                std.mem.indexOf(u8, proc.cmdline, pattern) != null)
            {
                try filtered_list.append(allocator, proc);
            }
        }

        filtered_procs = try filtered_list.toOwnedSlice(allocator);
    }
    defer if (filter_pattern != null) allocator.free(filtered_procs);

    // Sort processes
    switch (sort_by) {
        .cpu => {
            std.mem.sort(output.ProcessInfo, filtered_procs, {}, struct {
                fn lessThan(_: void, a: output.ProcessInfo, b: output.ProcessInfo) bool {
                    return a.cpu_percent > b.cpu_percent;
                }
            }.lessThan);
        },
        .mem => {
            std.mem.sort(output.ProcessInfo, filtered_procs, {}, struct {
                fn lessThan(_: void, a: output.ProcessInfo, b: output.ProcessInfo) bool {
                    return a.mem_rss > b.mem_rss;
                }
            }.lessThan);
        },
        .name => {
            std.mem.sort(output.ProcessInfo, filtered_procs, {}, struct {
                fn lessThan(_: void, a: output.ProcessInfo, b: output.ProcessInfo) bool {
                    return std.mem.order(u8, a.name, b.name) == .lt;
                }
            }.lessThan);
        },
    }

    try output.printProcesses(stdout, format, allocator, filtered_procs);
}

// Parse priority string to u8
fn parsePriority(priority_str: []const u8) ?u8 {
    if (std.mem.eql(u8, priority_str, "emerg")) return 0;
    if (std.mem.eql(u8, priority_str, "alert")) return 1;
    if (std.mem.eql(u8, priority_str, "crit")) return 2;
    if (std.mem.eql(u8, priority_str, "err") or std.mem.eql(u8, priority_str, "error")) return 3;
    if (std.mem.eql(u8, priority_str, "warn") or std.mem.eql(u8, priority_str, "warning")) return 4;
    if (std.mem.eql(u8, priority_str, "notice")) return 5;
    if (std.mem.eql(u8, priority_str, "info")) return 6;
    if (std.mem.eql(u8, priority_str, "debug")) return 7;

    return std.fmt.parseInt(u8, priority_str, 10) catch null;
}

// Command: sermon logs [--unit <name>] [--since <duration>] [--priority <level>]
pub fn cmdLogs(
    allocator: Allocator,
    storage: *Storage,
    format: output.OutputFormat,
    unit: ?[]const u8,
    since_seconds: ?i64,
    priority_str: ?[]const u8,
) !void {
    const f: std.fs.File = .{ .handle = std.posix.STDOUT_FILENO };
    const stdout = f.deprecatedWriter();

    const since_ts: ?i64 = if (since_seconds) |s| std.time.timestamp() - s else null;
    const priority: ?u8 = if (priority_str) |p| parsePriority(p) else null;

    const log_entries = try storage.queryLogs(since_ts, unit, priority);
    defer {
        for (log_entries) |entry| {
            allocator.free(entry.source);
            if (entry.unit) |u| allocator.free(u);
            allocator.free(entry.message);
        }
        if (log_entries.len > 0) allocator.free(log_entries);
    }

    if (log_entries.len == 0) {
        try stdout.print("No logs found\n", .{});
        return;
    }

    try output.printLogs(stdout, format, allocator, log_entries);
}

// Command: sermon query "<sql>"
pub fn cmdQuery(
    allocator: Allocator,
    storage: *Storage,
    format: output.OutputFormat,
    sql: []const u8,
) !void {
    const f: std.fs.File = .{ .handle = std.posix.STDOUT_FILENO };
    const stdout = f.deprecatedWriter();

    var result = try storage.rawQuery(sql);
    defer result.deinit();

    if (result.rows.len == 0) {
        try stdout.print("No results\n", .{});
        return;
    }

    try output.printQueryResult(
        stdout,
        format,
        allocator,
        result.columns,
        result.rows,
    );
}

// Parse duration string like "1h", "30m", "5d" to seconds
pub fn parseDuration(duration_str: []const u8) !i64 {
    if (duration_str.len < 2) return error.InvalidDuration;

    const value_str = duration_str[0 .. duration_str.len - 1];
    const value = try std.fmt.parseInt(i64, value_str, 10);
    const unit = duration_str[duration_str.len - 1];

    return switch (unit) {
        's' => value,
        'm' => value * 60,
        'h' => value * 60 * 60,
        'd' => value * 60 * 60 * 24,
        'w' => value * 60 * 60 * 24 * 7,
        else => error.InvalidDuration,
    };
}

test "parseDuration" {
    try std.testing.expectEqual(@as(i64, 30), try parseDuration("30s"));
    try std.testing.expectEqual(@as(i64, 1800), try parseDuration("30m"));
    try std.testing.expectEqual(@as(i64, 3600), try parseDuration("1h"));
    try std.testing.expectEqual(@as(i64, 86400), try parseDuration("1d"));
    try std.testing.expectEqual(@as(i64, 604800), try parseDuration("1w"));
}

test "parsePriority" {
    try std.testing.expectEqual(@as(?u8, 0), parsePriority("emerg"));
    try std.testing.expectEqual(@as(?u8, 3), parsePriority("err"));
    try std.testing.expectEqual(@as(?u8, 3), parsePriority("error"));
    try std.testing.expectEqual(@as(?u8, 4), parsePriority("warn"));
    try std.testing.expectEqual(@as(?u8, 7), parsePriority("debug"));
    try std.testing.expectEqual(@as(?u8, 5), parsePriority("5"));
}
