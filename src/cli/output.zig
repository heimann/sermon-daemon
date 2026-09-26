const std = @import("std");
const Allocator = std.mem.Allocator;

// Linux sysconf constants not exposed in Zig's std.c._SC enum
const _SC_NPROCESSORS_ONLN: c_int = 84;

const collector = @import("collector");
const logs_mod = @import("logs");

pub const SystemMetrics = collector.SystemMetrics;
pub const ProcessInfo = collector.ProcessInfo;
pub const DiskInfo = collector.DiskInfo;
pub const LogEntry = logs_mod.LogEntry;

pub const OutputFormat = enum {
    table,
    json,
    csv,
};

const Writer = std.fs.File.DeprecatedWriter;

fn repairUtf8(allocator: Allocator, value: []const u8) ![]u8 {
    var valid = std.ArrayList(u8){};
    errdefer valid.deinit(allocator);

    var i: usize = 0;
    while (i < value.len) {
        const sequence_len = std.unicode.utf8ByteSequenceLength(value[i]) catch {
            try valid.appendSlice(allocator, "\xef\xbf\xbd");
            i += 1;
            continue;
        };
        if (i + sequence_len > value.len) {
            try valid.appendSlice(allocator, "\xef\xbf\xbd");
            i += 1;
            continue;
        }
        _ = std.unicode.utf8Decode(value[i..][0..sequence_len]) catch {
            try valid.appendSlice(allocator, "\xef\xbf\xbd");
            i += 1;
            continue;
        };
        try valid.appendSlice(allocator, value[i .. i + sequence_len]);
        i += sequence_len;
    }

    return valid.toOwnedSlice(allocator);
}

fn writeJsonString(writer: Writer, allocator: Allocator, value: []const u8) !void {
    if (std.unicode.utf8ValidateSlice(value)) {
        try writer.print("{f}", .{std.json.fmt(value, .{ .escape_unicode = true })});
        return;
    }

    const valid = try repairUtf8(allocator, value);
    defer allocator.free(valid);
    try writer.print("{f}", .{std.json.fmt(valid, .{ .escape_unicode = true })});
}

fn writeJsonFloat(writer: Writer, value: f32) !void {
    if (std.math.isFinite(value)) {
        try writer.print("{d:.2}", .{value});
    } else {
        try writer.writeAll("null");
    }
}

fn isUnicodeTerminalControl(codepoint: u21) bool {
    return (codepoint >= 0x80 and codepoint <= 0x9f) or
        codepoint == 0x061c or
        codepoint == 0x200e or
        codepoint == 0x200f or
        (codepoint >= 0x202a and codepoint <= 0x202e) or
        (codepoint >= 0x2066 and codepoint <= 0x2069);
}

fn writeUnicodeEscape(writer: Writer, codepoint: u21) !usize {
    var buf: [16]u8 = undefined;
    const escaped = try std.fmt.bufPrint(&buf, "\\u{{{x}}}", .{codepoint});
    try writer.writeAll(escaped);
    return escaped.len;
}

fn writeCsvField(writer: Writer, value: []const u8) !void {
    try writer.writeByte('"');

    // Spreadsheet programs interpret these leading bytes as formulas even in
    // quoted CSV fields. Prefix an apostrophe so exporting hostile telemetry
    // never turns it into an executable spreadsheet cell.
    if (value.len > 0 and switch (value[0]) {
        '=', '+', '-', '@', '\t', '\r', '\n' => true,
        else => false,
    }) {
        try writer.writeByte('\'');
    }

    var i: usize = 0;
    while (i < value.len) {
        const byte = value[i];
        if (byte == '"') {
            try writer.writeAll("\"\"");
            i += 1;
            continue;
        }
        if ((byte < 0x20 and byte != '\r' and byte != '\n') or byte == 0x7f) {
            try writer.print("\\x{x:0>2}", .{byte});
            i += 1;
            continue;
        }
        if (byte < 0x80) {
            try writer.writeByte(byte);
            i += 1;
            continue;
        }

        const sequence_len = std.unicode.utf8ByteSequenceLength(byte) catch {
            try writer.print("\\x{x:0>2}", .{byte});
            i += 1;
            continue;
        };
        if (i + sequence_len > value.len) {
            try writer.print("\\x{x:0>2}", .{byte});
            i += 1;
            continue;
        }
        const codepoint = std.unicode.utf8Decode(value[i..][0..sequence_len]) catch {
            try writer.print("\\x{x:0>2}", .{byte});
            i += 1;
            continue;
        };
        if (isUnicodeTerminalControl(codepoint)) {
            _ = try writeUnicodeEscape(writer, codepoint);
        } else {
            try writer.writeAll(value[i .. i + sequence_len]);
        }
        i += sequence_len;
    }
    try writer.writeByte('"');
}

fn writeTerminalSafe(writer: Writer, value: []const u8, min_width: usize) !void {
    var rendered_len: usize = 0;
    var i: usize = 0;
    while (i < value.len) {
        const byte = value[i];
        if (byte < 0x20 or byte == 0x7f) {
            try writer.print("\\x{x:0>2}", .{byte});
            rendered_len += 4;
            i += 1;
            continue;
        }
        if (byte < 0x80) {
            try writer.writeByte(byte);
            rendered_len += 1;
            i += 1;
            continue;
        }

        const sequence_len = std.unicode.utf8ByteSequenceLength(byte) catch {
            try writer.print("\\x{x:0>2}", .{byte});
            rendered_len += 4;
            i += 1;
            continue;
        };
        if (i + sequence_len > value.len) {
            try writer.print("\\x{x:0>2}", .{byte});
            rendered_len += 4;
            i += 1;
            continue;
        }
        const codepoint = std.unicode.utf8Decode(value[i..][0..sequence_len]) catch {
            try writer.print("\\x{x:0>2}", .{byte});
            rendered_len += 4;
            i += 1;
            continue;
        };
        if (isUnicodeTerminalControl(codepoint)) {
            rendered_len += try writeUnicodeEscape(writer, codepoint);
        } else {
            try writer.writeAll(value[i .. i + sequence_len]);
            rendered_len += sequence_len;
        }
        i += sequence_len;
    }

    if (rendered_len < min_width) {
        try writer.writeByteNTimes(' ', min_width - rendered_len);
    }
}

// Format bytes to human-readable string (e.g., "1.5GB")
fn formatBytes(bytes: u64, buf: []u8) ![]const u8 {
    const kb: f64 = 1024.0;
    const mb = kb * 1024.0;
    const gb = mb * 1024.0;
    const tb = gb * 1024.0;

    const val = @as(f64, @floatFromInt(bytes));

    if (val >= tb) {
        return std.fmt.bufPrint(buf, "{d:.1}TB", .{val / tb});
    } else if (val >= gb) {
        return std.fmt.bufPrint(buf, "{d:.1}GB", .{val / gb});
    } else if (val >= mb) {
        return std.fmt.bufPrint(buf, "{d:.1}MB", .{val / mb});
    } else if (val >= kb) {
        return std.fmt.bufPrint(buf, "{d:.1}KB", .{val / kb});
    } else {
        return std.fmt.bufPrint(buf, "{}B", .{bytes});
    }
}

// Create a simple progress bar
fn makeProgressBar(percent: f32, width: usize, buf: []u8) []const u8 {
    const filled = @as(usize, @intFromFloat(@min(percent / 100.0 * @as(f32, @floatFromInt(width)), @as(f32, @floatFromInt(width)))));

    var i: usize = 0;
    while (i < width) : (i += 1) {
        buf[i] = if (i < filled) '#' else '-';
    }

    return buf[0..width];
}

// Status output (latest metrics)
pub fn printStatus(
    writer: Writer,
    format: OutputFormat,
    allocator: Allocator,
    hostname: []const u8,
    metrics: ?SystemMetrics,
    disks: []const DiskInfo,
    top_procs: []const ProcessInfo,
) !void {
    switch (format) {
        .table => try printStatusTable(writer, allocator, hostname, metrics, disks, top_procs),
        .json => try printStatusJson(writer, allocator, hostname, metrics, disks, top_procs),
        .csv => try printStatusCsv(writer, hostname, metrics, disks, top_procs),
    }
}

fn printStatusTable(
    writer: Writer,
    allocator: Allocator,
    hostname: []const u8,
    metrics: ?SystemMetrics,
    disks: []const DiskInfo,
    top_procs: []const ProcessInfo,
) !void {
    _ = allocator;

    try writer.writeAll("\nSERMON STATUS - ");
    try writeTerminalSafe(writer, hostname, 0);
    try writer.writeByte('\n');
    try writer.print("========================\n\n", .{});

    if (metrics) |m| {
        var buf: [64]u8 = undefined;
        var bar_buf: [30]u8 = undefined;

        // CPU
        const cpu_bar = makeProgressBar(m.cpu_percent, 30, &bar_buf);
        const num_cpus: u32 = @intCast(std.c.sysconf(_SC_NPROCESSORS_ONLN));
        try writer.print("CPU:    {d:5.1}% {s} ({d} cores)\n", .{ m.cpu_percent, cpu_bar, num_cpus });

        // Memory
        const mem_bar = makeProgressBar(m.mem_percent, 30, &bar_buf);
        const mem_used_str = try formatBytes(m.mem_used, buf[0..32]);
        const mem_total_str = try formatBytes(m.mem_total, buf[32..]);
        try writer.print("Memory: {d:5.1}% {s} ({s} / {s})\n", .{ m.mem_percent, mem_bar, mem_used_str, mem_total_str });

        // Swap
        const swap_percent = if (m.swap_total > 0)
            @as(f32, @floatFromInt(m.swap_used)) / @as(f32, @floatFromInt(m.swap_total)) * 100.0
        else
            0.0;
        const swap_bar = makeProgressBar(swap_percent, 30, &bar_buf);
        try writer.print("Swap:   {d:5.1}% {s}\n", .{ swap_percent, swap_bar });

        try writer.print("\n", .{});
    } else {
        try writer.print("No metrics available\n\n", .{});
    }

    // Disks
    if (disks.len > 0) {
        try writer.print("Disks:\n", .{});
        for (disks) |disk| {
            var buf: [64]u8 = undefined;
            const used_str = try formatBytes(disk.used_bytes, buf[0..32]);
            const total_str = try formatBytes(disk.total_bytes, buf[32..]);
            try writer.writeAll("  ");
            try writeTerminalSafe(writer, disk.mount_point, 12);
            try writer.print(" {d:5.1}%  ({s} / {s})\n", .{ disk.percent, used_str, total_str });
        }
        try writer.print("\n", .{});
    }

    // Top processes
    if (top_procs.len > 0) {
        try writer.print("Top Processes (by CPU):\n", .{});
        try writer.print("  {s:<8} {s:<7} {s:<9} {s}\n", .{ "PID", "CPU%", "MEM", "NAME" });

        for (top_procs) |proc| {
            var buf: [32]u8 = undefined;
            const mem_str = try formatBytes(proc.mem_rss, &buf);
            try writer.print("  {d:<8} {d:5.1}%  {s:<9} ", .{
                proc.pid,
                proc.cpu_percent,
                mem_str,
            });
            try writeTerminalSafe(writer, proc.name, 0);
            try writer.writeByte('\n');
        }
    }

    try writer.print("\n", .{});
}

fn printStatusJson(
    writer: Writer,
    allocator: Allocator,
    hostname: []const u8,
    metrics: ?SystemMetrics,
    disks: []const DiskInfo,
    top_procs: []const ProcessInfo,
) !void {
    try writer.print("{{\n", .{});
    try writer.writeAll("  \"hostname\": ");
    try writeJsonString(writer, allocator, hostname);
    try writer.writeAll(",\n");
    try writer.print("  \"timestamp\": {d},\n", .{std.time.timestamp()});

    if (metrics) |m| {
        try writer.writeAll("  \"cpu_percent\": ");
        try writeJsonFloat(writer, m.cpu_percent);
        try writer.writeAll(",\n  \"cpu_user\": ");
        try writeJsonFloat(writer, m.cpu_user);
        try writer.writeAll(",\n  \"cpu_system\": ");
        try writeJsonFloat(writer, m.cpu_system);
        try writer.writeAll(",\n  \"cpu_iowait\": ");
        try writeJsonFloat(writer, m.cpu_iowait);
        try writer.writeAll(",\n");
        try writer.print("  \"mem_total\": {d},\n", .{m.mem_total});
        try writer.print("  \"mem_used\": {d},\n", .{m.mem_used});
        try writer.writeAll("  \"mem_percent\": ");
        try writeJsonFloat(writer, m.mem_percent);
        try writer.writeAll(",\n");
        try writer.print("  \"swap_total\": {d},\n", .{m.swap_total});
        try writer.print("  \"swap_used\": {d},\n", .{m.swap_used});
    }

    // Disks
    try writer.print("  \"disks\": [\n", .{});
    for (disks, 0..) |disk, i| {
        try writer.print("    {{\n", .{});
        try writer.writeAll("      \"mount_point\": ");
        try writeJsonString(writer, allocator, disk.mount_point);
        try writer.writeAll(",\n      \"filesystem\": ");
        try writeJsonString(writer, allocator, disk.filesystem);
        try writer.writeAll(",\n");
        try writer.print("      \"total_bytes\": {d},\n", .{disk.total_bytes});
        try writer.print("      \"used_bytes\": {d},\n", .{disk.used_bytes});
        try writer.writeAll("      \"percent\": ");
        try writeJsonFloat(writer, disk.percent);
        try writer.writeByte('\n');
        try writer.print("    }}{s}\n", .{if (i < disks.len - 1) "," else ""});
    }
    try writer.print("  ],\n", .{});

    // Top processes
    try writer.print("  \"top_processes\": [\n", .{});
    for (top_procs, 0..) |proc, i| {
        try writer.print("    {{\n", .{});
        try writer.print("      \"pid\": {d},\n", .{proc.pid});
        try writer.writeAll("      \"name\": ");
        try writeJsonString(writer, allocator, proc.name);
        try writer.writeAll(",\n");
        try writer.writeAll("      \"cpu_percent\": ");
        try writeJsonFloat(writer, proc.cpu_percent);
        try writer.writeAll(",\n");
        try writer.print("      \"mem_rss\": {d},\n", .{proc.mem_rss});
        try writer.print("      \"threads\": {d},\n", .{proc.threads});
        try writer.writeAll("      \"username\": ");
        try writeJsonString(writer, allocator, proc.username);
        try writer.writeByte('\n');
        try writer.print("    }}{s}\n", .{if (i < top_procs.len - 1) "," else ""});
    }
    try writer.print("  ]\n", .{});

    try writer.print("}}\n", .{});
}

fn printStatusCsv(
    writer: Writer,
    hostname: []const u8,
    metrics: ?SystemMetrics,
    disks: []const DiskInfo,
    top_procs: []const ProcessInfo,
) !void {
    _ = disks;
    _ = top_procs;

    try writer.print("hostname,timestamp,cpu_percent,cpu_user,cpu_system,cpu_iowait,mem_total,mem_used,mem_percent,swap_total,swap_used\r\n", .{});

    if (metrics) |m| {
        try writeCsvField(writer, hostname);
        try writer.print(",{d},{d:.2},{d:.2},{d:.2},{d:.2},{d},{d},{d:.2},{d},{d}\r\n", .{
            std.time.timestamp(),
            m.cpu_percent,
            m.cpu_user,
            m.cpu_system,
            m.cpu_iowait,
            m.mem_total,
            m.mem_used,
            m.mem_percent,
            m.swap_total,
            m.swap_used,
        });
    }
}

// Metrics time series output
pub fn printMetrics(
    writer: Writer,
    format: OutputFormat,
    allocator: Allocator,
    metrics: []const SystemMetrics,
) !void {
    _ = allocator;

    switch (format) {
        .table => try printMetricsTable(writer, metrics),
        .json => try printMetricsJson(writer, metrics),
        .csv => try printMetricsCsv(writer, metrics),
    }
}

fn printMetricsTable(writer: Writer, metrics: []const SystemMetrics) !void {
    try writer.print("\n{s:>7} {s:>7} {s:>8} {s:>7}\n", .{ "CPU%", "USER%", "IOWAIT%", "MEM%" });
    try writer.print("{s}\n", .{"-" ** 40});

    for (metrics) |m| {
        try writer.print("{d:6.1}  {d:6.1}  {d:7.1}  {d:6.1}\n", .{
            m.cpu_percent,
            m.cpu_user,
            m.cpu_iowait,
            m.mem_percent,
        });
    }

    try writer.print("\n", .{});
}

fn printMetricsJson(writer: Writer, metrics: []const SystemMetrics) !void {
    try writer.print("[\n", .{});

    for (metrics, 0..) |m, i| {
        try writer.print("  {{\n", .{});
        try writer.writeAll("    \"cpu_percent\": ");
        try writeJsonFloat(writer, m.cpu_percent);
        try writer.writeAll(",\n    \"cpu_user\": ");
        try writeJsonFloat(writer, m.cpu_user);
        try writer.writeAll(",\n    \"cpu_system\": ");
        try writeJsonFloat(writer, m.cpu_system);
        try writer.writeAll(",\n    \"cpu_iowait\": ");
        try writeJsonFloat(writer, m.cpu_iowait);
        try writer.writeAll(",\n");
        try writer.print("    \"mem_total\": {d},\n", .{m.mem_total});
        try writer.print("    \"mem_used\": {d},\n", .{m.mem_used});
        try writer.writeAll("    \"mem_percent\": ");
        try writeJsonFloat(writer, m.mem_percent);
        try writer.writeAll(",\n");
        try writer.print("    \"swap_total\": {d},\n", .{m.swap_total});
        try writer.print("    \"swap_used\": {d}\n", .{m.swap_used});
        try writer.print("  }}{s}\n", .{if (i < metrics.len - 1) "," else ""});
    }

    try writer.print("]\n", .{});
}

fn printMetricsCsv(writer: Writer, metrics: []const SystemMetrics) !void {
    try writer.print("cpu_percent,cpu_user,cpu_system,cpu_iowait,mem_total,mem_used,mem_percent,swap_total,swap_used\r\n", .{});

    for (metrics) |m| {
        try writer.print("{d:.2},{d:.2},{d:.2},{d:.2},{d},{d},{d:.2},{d},{d}\r\n", .{
            m.cpu_percent,
            m.cpu_user,
            m.cpu_system,
            m.cpu_iowait,
            m.mem_total,
            m.mem_used,
            m.mem_percent,
            m.swap_total,
            m.swap_used,
        });
    }
}

// Process list output
pub fn printProcesses(
    writer: Writer,
    format: OutputFormat,
    allocator: Allocator,
    processes: []const ProcessInfo,
) !void {
    switch (format) {
        .table => try printProcessesTable(writer, processes),
        .json => try printProcessesJson(writer, allocator, processes),
        .csv => try printProcessesCsv(writer, processes),
    }
}

fn printProcessesTable(writer: Writer, processes: []const ProcessInfo) !void {
    try writer.print("\n{s:<8} {s:>7} {s:>9} {s:>6} {s}\n", .{ "PID", "CPU%", "MEM", "STATE", "NAME" });
    try writer.print("{s}\n", .{"-" ** 60});

    for (processes) |proc| {
        var buf: [32]u8 = undefined;
        const mem_str = try formatBytes(proc.mem_rss, &buf);

        try writer.print("{d:<8} {d:6.1}  {s:>9} ", .{
            proc.pid,
            proc.cpu_percent,
            mem_str,
        });
        try writeTerminalSafe(writer, &.{proc.state}, 6);
        try writer.writeByte(' ');
        try writeTerminalSafe(writer, proc.name, 0);
        try writer.writeByte('\n');
    }

    try writer.print("\n", .{});
}

fn printProcessesJson(writer: Writer, allocator: Allocator, processes: []const ProcessInfo) !void {
    try writer.print("[\n", .{});

    for (processes, 0..) |proc, i| {
        try writer.print("  {{\n", .{});
        try writer.print("    \"pid\": {d},\n", .{proc.pid});
        try writer.writeAll("    \"name\": ");
        try writeJsonString(writer, allocator, proc.name);
        try writer.writeAll(",\n    \"cmdline\": ");
        try writeJsonString(writer, allocator, proc.cmdline);
        try writer.writeAll(",\n    \"state\": ");
        try writeJsonString(writer, allocator, &.{proc.state});
        try writer.writeAll(",\n");
        try writer.writeAll("    \"cpu_percent\": ");
        try writeJsonFloat(writer, proc.cpu_percent);
        try writer.writeAll(",\n");
        try writer.print("    \"mem_rss\": {d},\n", .{proc.mem_rss});
        try writer.print("    \"threads\": {d},\n", .{proc.threads});
        try writer.writeAll("    \"username\": ");
        try writeJsonString(writer, allocator, proc.username);
        try writer.writeAll(",\n");
        try writer.print("    \"io_read_bytes\": {d},\n", .{proc.io_read_bytes});
        try writer.print("    \"io_write_bytes\": {d},\n", .{proc.io_write_bytes});
        try writer.writeAll("    \"cgroup\": ");
        try writeJsonString(writer, allocator, proc.cgroup);
        try writer.writeAll(",\n    \"unit\": ");
        try writeJsonString(writer, allocator, proc.unit);
        try writer.writeByte('\n');
        try writer.print("  }}{s}\n", .{if (i < processes.len - 1) "," else ""});
    }

    try writer.print("]\n", .{});
}

fn printProcessesCsv(writer: Writer, processes: []const ProcessInfo) !void {
    try writer.print("pid,name,cmdline,state,cpu_percent,mem_rss,threads,username,io_read_bytes,io_write_bytes,cgroup,unit\r\n", .{});

    for (processes) |proc| {
        try writer.print("{d},", .{proc.pid});
        try writeCsvField(writer, proc.name);
        try writer.writeByte(',');
        try writeCsvField(writer, proc.cmdline);
        try writer.writeByte(',');
        try writeCsvField(writer, &.{proc.state});
        try writer.print(",{d:.2},{d},{d},", .{
            proc.cpu_percent,
            proc.mem_rss,
            proc.threads,
        });
        try writeCsvField(writer, proc.username);
        try writer.print(",{d},{d},", .{
            proc.io_read_bytes,
            proc.io_write_bytes,
        });
        try writeCsvField(writer, proc.cgroup);
        try writer.writeByte(',');
        try writeCsvField(writer, proc.unit);
        try writer.writeAll("\r\n");
    }
}

// Log entries output
pub fn printLogs(
    writer: Writer,
    format: OutputFormat,
    allocator: Allocator,
    logs: []const LogEntry,
) !void {
    switch (format) {
        .table => try printLogsTable(writer, logs),
        .json => try printLogsJson(writer, allocator, logs),
        .csv => try printLogsCsv(writer, logs),
    }
}

fn printLogsTable(writer: Writer, logs: []const LogEntry) !void {
    try writer.print("\n", .{});

    for (logs) |log| {
        // Format timestamp
        const epoch_sec: i64 = log.timestamp;
        const t = std.time.epoch.EpochSeconds{ .secs = @intCast(epoch_sec) };
        const day_sec = t.getDaySeconds();
        const h = day_sec.getHoursIntoDay();
        const min = day_sec.getMinutesIntoHour();
        const sec = day_sec.getSecondsIntoMinute();

        const unit_str = log.unit orelse "system";
        const priority_str = priorityToString(log.priority);

        try writer.print("{d:02}:{d:02}:{d:02} [", .{
            h,
            min,
            sec,
        });
        try writeTerminalSafe(writer, unit_str, 0);
        try writer.print("] {s}: ", .{priority_str});
        try writeTerminalSafe(writer, log.message, 0);
        try writer.writeByte('\n');
    }

    try writer.print("\n", .{});
}

fn printLogsJson(writer: Writer, allocator: Allocator, logs: []const LogEntry) !void {
    try writer.print("[\n", .{});

    for (logs, 0..) |log, i| {
        try writer.print("  {{\n", .{});
        try writer.print("    \"timestamp\": {d},\n", .{log.timestamp});
        try writer.writeAll("    \"source\": ");
        try writeJsonString(writer, allocator, log.source);
        try writer.writeAll(",\n");

        if (log.unit) |unit| {
            try writer.writeAll("    \"unit\": ");
            try writeJsonString(writer, allocator, unit);
            try writer.writeAll(",\n");
        } else {
            try writer.print("    \"unit\": null,\n", .{});
        }

        if (log.identifier) |identifier| {
            try writer.writeAll("    \"identifier\": ");
            try writeJsonString(writer, allocator, identifier);
            try writer.writeAll(",\n");
        } else {
            try writer.print("    \"identifier\": null,\n", .{});
        }

        if (log.systemd_unit) |systemd_unit| {
            try writer.writeAll("    \"systemd_unit\": ");
            try writeJsonString(writer, allocator, systemd_unit);
            try writer.writeAll(",\n");
        } else {
            try writer.print("    \"systemd_unit\": null,\n", .{});
        }

        try writer.print("    \"priority\": {d},\n", .{log.priority});
        try writer.writeAll("    \"message\": ");
        try writeJsonString(writer, allocator, log.message);

        if (log.pid) |pid| {
            try writer.print(",\n    \"pid\": {d}\n", .{pid});
        } else {
            try writer.print("\n", .{});
        }

        try writer.print("  }}{s}\n", .{if (i < logs.len - 1) "," else ""});
    }

    try writer.print("]\n", .{});
}

fn printLogsCsv(writer: Writer, logs: []const LogEntry) !void {
    try writer.print("timestamp,source,unit,identifier,systemd_unit,priority,message,pid\r\n", .{});

    for (logs) |log| {
        const unit_str = log.unit orelse "";
        const identifier_str = log.identifier orelse "";
        const systemd_unit_str = log.systemd_unit orelse "";
        const pid_str = if (log.pid) |pid| pid else 0;

        try writer.print("{d},", .{log.timestamp});
        try writeCsvField(writer, log.source);
        try writer.writeByte(',');
        try writeCsvField(writer, unit_str);
        try writer.writeByte(',');
        try writeCsvField(writer, identifier_str);
        try writer.writeByte(',');
        try writeCsvField(writer, systemd_unit_str);
        try writer.print(",{d},", .{log.priority});
        try writeCsvField(writer, log.message);
        try writer.print(",{d}\r\n", .{pid_str});
    }
}

fn priorityToString(priority: u8) []const u8 {
    return switch (priority) {
        0 => "EMERG",
        1 => "ALERT",
        2 => "CRIT",
        3 => "ERROR",
        4 => "WARN",
        5 => "NOTICE",
        6 => "INFO",
        7 => "DEBUG",
        else => "UNKNOWN",
    };
}

// Generic query result output (for raw SQL queries)
pub fn printQueryResult(
    writer: Writer,
    format: OutputFormat,
    allocator: Allocator,
    columns: []const []const u8,
    rows: []const []?[]const u8,
) !void {
    switch (format) {
        .table => try printQueryTable(writer, columns, rows),
        .json => try printQueryJson(writer, allocator, columns, rows),
        .csv => try printQueryCsv(writer, columns, rows),
    }
}

fn printQueryTable(writer: Writer, columns: []const []const u8, rows: []const []?[]const u8) !void {
    try writer.print("\n", .{});

    for (columns) |col| {
        try writeTerminalSafe(writer, col, 20);
        try writer.writeByte(' ');
    }
    try writer.print("\n", .{});

    for (columns) |_| {
        try writer.print("{s} ", .{"-" ** 20});
    }
    try writer.print("\n", .{});

    for (rows) |row| {
        for (row) |cell| {
            const val = cell orelse "NULL";
            try writeTerminalSafe(writer, val, 20);
            try writer.writeByte(' ');
        }
        try writer.print("\n", .{});
    }

    try writer.print("\n", .{});
}

fn uniqueJsonKey(allocator: Allocator, used_keys: *const std.StringHashMap(void), column: []const u8) ![]u8 {
    const base = if (std.unicode.utf8ValidateSlice(column))
        try allocator.dupe(u8, column)
    else
        try repairUtf8(allocator, column);
    if (!used_keys.contains(base)) return base;
    defer allocator.free(base);

    var suffix: usize = 2;
    while (true) : (suffix += 1) {
        const candidate = try std.fmt.allocPrint(allocator, "{s}#{d}", .{ base, suffix });
        if (!used_keys.contains(candidate)) return candidate;
        allocator.free(candidate);
    }
}

fn printQueryJson(writer: Writer, allocator: Allocator, columns: []const []const u8, rows: []const []?[]const u8) !void {
    var keys = try allocator.alloc([]u8, columns.len);
    defer allocator.free(keys);
    var initialized_keys: usize = 0;
    defer for (keys[0..initialized_keys]) |key| allocator.free(key);

    var used_keys = std.StringHashMap(void).init(allocator);
    defer used_keys.deinit();

    // JSON objects cannot represent duplicate column aliases without silently
    // dropping values in common parsers, so assign later duplicates stable keys.
    for (columns, 0..) |column, i| {
        const key = try uniqueJsonKey(allocator, &used_keys, column);
        errdefer allocator.free(key);
        try used_keys.put(key, {});
        keys[i] = key;
        initialized_keys += 1;
    }

    try writer.print("[\n", .{});

    for (rows, 0..) |row, i| {
        try writer.print("  {{\n", .{});

        const field_count = @min(keys.len, row.len);
        for (keys[0..field_count], row[0..field_count], 0..) |key, cell, j| {
            if (cell) |val| {
                try writer.writeAll("    ");
                try writeJsonString(writer, allocator, key);
                try writer.writeAll(": ");
                try writeJsonString(writer, allocator, val);
                try writer.print("{s}\n", .{if (j + 1 < field_count) "," else ""});
            } else {
                try writer.writeAll("    ");
                try writeJsonString(writer, allocator, key);
                try writer.print(": null{s}\n", .{if (j + 1 < field_count) "," else ""});
            }
        }

        try writer.print("  }}{s}\n", .{if (i < rows.len - 1) "," else ""});
    }

    try writer.print("]\n", .{});
}

fn printQueryCsv(writer: Writer, columns: []const []const u8, rows: []const []?[]const u8) !void {
    for (columns, 0..) |col, i| {
        try writeCsvField(writer, col);
        try writer.writeAll(if (i < columns.len - 1) "," else "\r\n");
    }

    for (rows) |row| {
        for (row, 0..) |cell, i| {
            const val = cell orelse "";
            try writeCsvField(writer, val);
            try writer.writeAll(if (i < row.len - 1) "," else "\r\n");
        }
    }
}

const hostile_text = "quote\" slash\\ comma, cr\r lf\n ansi\x1b]0;owned\x07 c1\u{009b} bidi\u{202e} unicode café";

fn testProcess(name: []const u8, state: u8) ProcessInfo {
    return .{
        .pid = 42,
        .name = name,
        .cmdline = hostile_text,
        .state = state,
        .cpu_percent = 1.25,
        .mem_rss = 4096,
        .threads = 2,
        .username = hostile_text,
        .io_read_bytes = 10,
        .io_write_bytes = 20,
        .cgroup = hostile_text,
        .unit = hostile_text,
    };
}

fn testLog(message: []const u8) LogEntry {
    return .{
        .timestamp = 1_700_000_000,
        .source = hostile_text,
        .unit = hostile_text,
        .identifier = hostile_text,
        .systemd_unit = hostile_text,
        .priority = 6,
        .message = message,
        .pid = 42,
    };
}

test "all JSON modes encode hostile strings and remain parseable" {
    const testing = std.testing;
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const disks = [_]DiskInfo{.{
        .mount_point = hostile_text,
        .filesystem = hostile_text,
        .total_bytes = 100,
        .used_bytes = 50,
        .percent = 50,
    }};
    const processes = [_]ProcessInfo{testProcess(hostile_text, '\x1b')};
    const logs = [_]LogEntry{testLog(hostile_text)};
    const columns = [_][]const u8{hostile_text};
    var row = [_]?[]const u8{hostile_text};
    const rows = [_][]?[]const u8{&row};

    {
        var file = try tmp.dir.createFile("status.json", .{});
        try printStatus(file.deprecatedWriter(), .json, allocator, hostile_text, null, &disks, &processes);
        file.close();
        const bytes = try tmp.dir.readFileAlloc(allocator, "status.json", 64 * 1024);
        defer allocator.free(bytes);
        try testing.expect(std.mem.indexOf(u8, bytes, "\u{009b}") == null);
        try testing.expect(std.mem.indexOf(u8, bytes, "\u{202e}") == null);
        try testing.expect(std.mem.indexOf(u8, bytes, "\\u009b") != null);
        try testing.expect(std.mem.indexOf(u8, bytes, "\\u202e") != null);
        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
        defer parsed.deinit();
        try testing.expectEqualStrings(hostile_text, parsed.value.object.get("hostname").?.string);
        try testing.expectEqualStrings(hostile_text, parsed.value.object.get("disks").?.array.items[0].object.get("mount_point").?.string);
        try testing.expectEqualStrings(hostile_text, parsed.value.object.get("top_processes").?.array.items[0].object.get("name").?.string);
    }

    {
        var file = try tmp.dir.createFile("processes.json", .{});
        try printProcesses(file.deprecatedWriter(), .json, allocator, &processes);
        file.close();
        const bytes = try tmp.dir.readFileAlloc(allocator, "processes.json", 64 * 1024);
        defer allocator.free(bytes);
        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
        defer parsed.deinit();
        const process = parsed.value.array.items[0].object;
        try testing.expectEqualStrings(hostile_text, process.get("name").?.string);
        try testing.expectEqualStrings(hostile_text, process.get("cmdline").?.string);
        try testing.expectEqualStrings("\x1b", process.get("state").?.string);
        try testing.expectEqualStrings(hostile_text, process.get("username").?.string);
        try testing.expectEqualStrings(hostile_text, process.get("cgroup").?.string);
        try testing.expectEqualStrings(hostile_text, process.get("unit").?.string);
    }

    {
        var file = try tmp.dir.createFile("logs.json", .{});
        try printLogs(file.deprecatedWriter(), .json, allocator, &logs);
        file.close();
        const bytes = try tmp.dir.readFileAlloc(allocator, "logs.json", 64 * 1024);
        defer allocator.free(bytes);
        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
        defer parsed.deinit();
        const log = parsed.value.array.items[0].object;
        try testing.expectEqualStrings(hostile_text, log.get("source").?.string);
        try testing.expectEqualStrings(hostile_text, log.get("unit").?.string);
        try testing.expectEqualStrings(hostile_text, log.get("identifier").?.string);
        try testing.expectEqualStrings(hostile_text, log.get("systemd_unit").?.string);
        try testing.expectEqualStrings(hostile_text, log.get("message").?.string);
    }

    {
        var file = try tmp.dir.createFile("query.json", .{});
        try printQueryResult(file.deprecatedWriter(), .json, allocator, &columns, &rows);
        file.close();
        const bytes = try tmp.dir.readFileAlloc(allocator, "query.json", 64 * 1024);
        defer allocator.free(bytes);
        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
        defer parsed.deinit();
        try testing.expectEqualStrings(hostile_text, parsed.value.array.items[0].object.get(hostile_text).?.string);
    }
}

test "query JSON disambiguates duplicate and repaired column names" {
    const testing = std.testing;
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const columns = [_][]const u8{ "x", "x", "x#2", "bad\xff", "bad\xfe" };
    var full_row = [_]?[]const u8{ "one", "two", "three", "four", "five" };
    var short_row = [_]?[]const u8{"only"};
    const rows = [_][]?[]const u8{ &full_row, &short_row };
    var file = try tmp.dir.createFile("duplicate-columns.json", .{});
    try printQueryResult(file.deprecatedWriter(), .json, allocator, &columns, &rows);
    file.close();

    const bytes = try tmp.dir.readFileAlloc(allocator, "duplicate-columns.json", 4096);
    defer allocator.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();

    const row = parsed.value.array.items[0].object;
    try testing.expectEqual(@as(usize, 5), row.count());
    try testing.expectEqualStrings("one", row.get("x").?.string);
    try testing.expectEqualStrings("two", row.get("x#2").?.string);
    try testing.expectEqualStrings("three", row.get("x#2#2").?.string);
    try testing.expectEqualStrings("four", row.get("bad\xef\xbf\xbd").?.string);
    try testing.expectEqualStrings("five", row.get("bad\xef\xbf\xbd#2").?.string);
    try testing.expectEqual(@as(usize, 1), parsed.value.array.items[1].object.count());
}

test "JSON string output preserves type for invalid UTF-8" {
    const testing = std.testing;
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const columns = [_][]const u8{"bad\xffkey"};
    var row = [_]?[]const u8{"bad\xfevalue"};
    const rows = [_][]?[]const u8{&row};
    var file = try tmp.dir.createFile("invalid.json", .{});
    try printQueryResult(file.deprecatedWriter(), .json, allocator, &columns, &rows);
    file.close();

    const bytes = try tmp.dir.readFileAlloc(allocator, "invalid.json", 4096);
    defer allocator.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
    defer parsed.deinit();
    const value = parsed.value.array.items[0].object.get("bad\xef\xbf\xbdkey").?;
    try testing.expectEqualStrings("bad\xef\xbf\xbdvalue", value.string);
}

test "JSON modes encode non-finite telemetry as null" {
    const testing = std.testing;
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const non_finite = SystemMetrics{
        .cpu_percent = std.math.nan(f32),
        .cpu_user = std.math.inf(f32),
        .cpu_system = -std.math.inf(f32),
        .cpu_iowait = std.math.nan(f32),
        .mem_total = 1,
        .mem_used = 1,
        .mem_percent = std.math.inf(f32),
        .swap_total = 1,
        .swap_used = 1,
    };
    const disks = [_]DiskInfo{.{
        .mount_point = "/",
        .filesystem = "ext4",
        .total_bytes = 1,
        .used_bytes = 1,
        .percent = std.math.nan(f32),
    }};
    var process = testProcess("safe", 'S');
    process.cpu_percent = std.math.inf(f32);

    {
        var file = try tmp.dir.createFile("non-finite-status.json", .{});
        try printStatus(file.deprecatedWriter(), .json, allocator, "host", non_finite, &disks, &.{process});
        file.close();
        const bytes = try tmp.dir.readFileAlloc(allocator, "non-finite-status.json", 16 * 1024);
        defer allocator.free(bytes);
        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
        defer parsed.deinit();
        try testing.expectEqual(std.json.Value.null, parsed.value.object.get("cpu_percent").?);
        try testing.expectEqual(std.json.Value.null, parsed.value.object.get("mem_percent").?);
        try testing.expectEqual(std.json.Value.null, parsed.value.object.get("disks").?.array.items[0].object.get("percent").?);
        try testing.expectEqual(std.json.Value.null, parsed.value.object.get("top_processes").?.array.items[0].object.get("cpu_percent").?);
    }

    {
        var file = try tmp.dir.createFile("non-finite-metrics.json", .{});
        try printMetrics(file.deprecatedWriter(), .json, allocator, &.{non_finite});
        file.close();
        const bytes = try tmp.dir.readFileAlloc(allocator, "non-finite-metrics.json", 16 * 1024);
        defer allocator.free(bytes);
        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
        defer parsed.deinit();
        try testing.expectEqual(std.json.Value.null, parsed.value.array.items[0].object.get("cpu_user").?);
    }

    {
        var file = try tmp.dir.createFile("non-finite-process.json", .{});
        try printProcesses(file.deprecatedWriter(), .json, allocator, &.{process});
        file.close();
        const bytes = try tmp.dir.readFileAlloc(allocator, "non-finite-process.json", 16 * 1024);
        defer allocator.free(bytes);
        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, bytes, .{});
        defer parsed.deinit();
        try testing.expectEqual(std.json.Value.null, parsed.value.array.items[0].object.get("cpu_percent").?);
    }
}

test "all CSV modes quote fields and neutralize formula-leading cells" {
    const testing = std.testing;
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const formula = "=SUM(1,2)\"\r\nnext";
    const processes = [_]ProcessInfo{testProcess(formula, '+')};
    const logs = [_]LogEntry{testLog(formula)};
    const columns = [_][]const u8{formula};
    var row = [_]?[]const u8{formula};
    const rows = [_][]?[]const u8{&row};

    var file = try tmp.dir.createFile("all.csv", .{});
    const writer = file.deprecatedWriter();
    try printStatus(writer, .csv, allocator, formula, .{
        .cpu_percent = 1,
        .cpu_user = 1,
        .cpu_system = 1,
        .cpu_iowait = 1,
        .mem_total = 1,
        .mem_used = 1,
        .mem_percent = 1,
        .swap_total = 1,
        .swap_used = 1,
    }, &.{}, &.{});
    try printProcesses(writer, .csv, allocator, &processes);
    try printLogs(writer, .csv, allocator, &logs);
    try printQueryResult(writer, .csv, allocator, &columns, &rows);
    file.close();

    const bytes = try tmp.dir.readFileAlloc(allocator, "all.csv", 64 * 1024);
    defer allocator.free(bytes);
    try testing.expect(std.mem.count(u8, bytes, "\"'=SUM(1,2)\"\"\r\nnext\"") >= 5);
    try testing.expect(std.mem.startsWith(u8, bytes, "hostname,timestamp"));
    try testing.expect(std.mem.endsWith(u8, bytes, "\r\n"));
    try testing.expect(std.mem.indexOfScalar(u8, bytes, '\x1b') == null);
    try testing.expect(std.mem.indexOfScalar(u8, bytes, '\x07') == null);
    try testing.expect(std.mem.indexOf(u8, bytes, "\\x1b]0;owned\\x07") != null);
    try testing.expect(std.mem.indexOf(u8, bytes, "\\u{202e}") != null);
}

test "all table modes escape terminal controls and preserve Unicode" {
    const testing = std.testing;
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const disks = [_]DiskInfo{.{
        .mount_point = hostile_text,
        .filesystem = hostile_text,
        .total_bytes = 100,
        .used_bytes = 50,
        .percent = 50,
    }};
    const processes = [_]ProcessInfo{testProcess(hostile_text, '\x1b')};
    const logs = [_]LogEntry{testLog(hostile_text)};
    const columns = [_][]const u8{hostile_text};
    var row = [_]?[]const u8{hostile_text};
    const rows = [_][]?[]const u8{&row};

    var file = try tmp.dir.createFile("all.txt", .{});
    const writer = file.deprecatedWriter();
    try printStatus(writer, .table, allocator, hostile_text, null, &disks, &processes);
    try printProcesses(writer, .table, allocator, &processes);
    try printLogs(writer, .table, allocator, &logs);
    try printQueryResult(writer, .table, allocator, &columns, &rows);
    file.close();

    const bytes = try tmp.dir.readFileAlloc(allocator, "all.txt", 64 * 1024);
    defer allocator.free(bytes);
    try testing.expect(std.mem.indexOfScalar(u8, bytes, '\x1b') == null);
    try testing.expect(std.mem.indexOfScalar(u8, bytes, '\x07') == null);
    try testing.expect(std.mem.indexOfScalar(u8, bytes, '\r') == null);
    try testing.expect(std.mem.indexOf(u8, bytes, "\u{202e}") == null);
    try testing.expect(std.mem.indexOf(u8, bytes, "\\x1b]0;owned\\x07") != null);
    try testing.expect(std.mem.indexOf(u8, bytes, "cr\\x0d lf\\x0a") != null);
    try testing.expect(std.mem.indexOf(u8, bytes, "bidi\\u{202e}") != null);
    try testing.expect(std.mem.indexOf(u8, bytes, "café") != null);
}
