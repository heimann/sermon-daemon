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

const hex_digits = "0123456789abcdef";

/// Length of the well-formed UTF-8 sequence starting at `s[i]`, or 0 if the
/// bytes there are not one (stray continuation, overlong, surrogate, truncated).
fn utf8SeqLen(s: []const u8, i: usize) usize {
    const n = std.unicode.utf8ByteSequenceLength(s[i]) catch return 0;
    if (i + n > s.len) return 0;
    _ = std.unicode.utf8Decode(s[i..][0..n]) catch return 0;
    return n;
}

/// Length of the unit at `s[i]` that writeSafe passes through unchanged, or 0
/// if it must be escaped: C0 controls, DEL, C1 controls (raw 0x80-0x9f bytes
/// and UTF-8 U+0080..U+009F) and bytes that are not valid UTF-8.
fn plainLen(s: []const u8, i: usize) usize {
    const c = s[i];
    if (c < 0x80) return if (c < 0x20 or c == 0x7f) 0 else 1;
    const n = utf8SeqLen(s, i);
    if (n == 2 and c == 0xc2 and s[i + 1] < 0xa0) return 0;
    return n;
}

/// Escapes (as visible `\xNN`, per byte) exactly what plainLen rejects.
fn writeSafe(writer: anytype, s: []const u8) !void {
    var i: usize = 0;
    while (i < s.len) {
        const n = plainLen(s, i);
        if (n > 0) {
            try writer.writeAll(s[i..][0..n]);
            i += n;
            continue;
        }
        const c = s[i];
        try writer.writeAll(&[_]u8{ '\\', 'x', hex_digits[c >> 4], hex_digits[c & 0xf] });
        i += 1;
    }
}

fn safeLen(s: []const u8) usize {
    var len: usize = 0;
    var i: usize = 0;
    while (i < s.len) {
        const n = plainLen(s, i);
        if (n > 0) {
            // Columns are counted in bytes, matching `{s:<N}` for valid UTF-8.
            len += n;
            i += n;
        } else {
            len += 4;
            i += 1;
        }
    }
    return len;
}

/// Pads after sanitizing: `{s:<N}` counts raw bytes, which would misalign
/// columns once control bytes expand to four-character escapes.
fn writeCell(writer: anytype, s: []const u8, width: usize, align_right: bool) !void {
    const len = safeLen(s);
    const pad = if (len < width) width - len else 0;
    if (align_right) try writer.writeByteNTimes(' ', pad);
    try writeSafe(writer, s);
    if (!align_right) try writer.writeByteNTimes(' ', pad);
}

fn writeJsonString(writer: anytype, s: []const u8) !void {
    try writer.writeByte('"');
    var start: usize = 0;
    var i: usize = 0;
    while (i < s.len) {
        const c = s[i];
        if (c >= 0x80) {
            const n = utf8SeqLen(s, i);
            if (n > 0) {
                i += n;
                continue;
            }
            try writer.writeAll(s[start..i]);
            try writer.writeAll("\xef\xbf\xbd");
            i += 1;
            start = i;
            continue;
        }
        if (c >= 0x20 and c != '"' and c != '\\') {
            i += 1;
            continue;
        }
        try writer.writeAll(s[start..i]);
        i += 1;
        start = i;
        switch (c) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            else => try writer.writeAll(&[_]u8{ '\\', 'u', '0', '0', hex_digits[c >> 4], hex_digits[c & 0xf] }),
        }
    }
    try writer.writeAll(s[start..]);
    try writer.writeByte('"');
}

fn jsonStr(writer: anytype, prefix: []const u8, value: []const u8, suffix: []const u8) !void {
    try writer.writeAll(prefix);
    try writeJsonString(writer, value);
    try writer.writeAll(suffix);
}

/// RFC 4180 field: always quoted, embedded quotes doubled. CR/LF stay as-is
/// because they are legal inside a quoted field.
fn writeCsvField(writer: anytype, s: []const u8) !void {
    try writer.writeByte('"');
    var start: usize = 0;
    for (s, 0..) |c, i| {
        if (c != '"') continue;
        try writer.writeAll(s[start .. i + 1]);
        start = i;
    }
    try writer.writeAll(s[start..]);
    try writer.writeByte('"');
}

fn stateChar(c: u8) u8 {
    return if (c < 0x20 or c > 0x7e) '?' else c;
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
    writer: anytype,
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
    writer: anytype,
    allocator: Allocator,
    hostname: []const u8,
    metrics: ?SystemMetrics,
    disks: []const DiskInfo,
    top_procs: []const ProcessInfo,
) !void {
    _ = allocator;

    try writer.writeAll("\nSERMON STATUS - ");
    try writeSafe(writer, hostname);
    try writer.writeAll("\n");
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
            try writeCell(writer, disk.mount_point, 12, false);
            try writer.print(" {d:5.1}%  ({s} / {s})\n", .{
                disk.percent,
                used_str,
                total_str,
            });
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
            try writeSafe(writer, proc.name);
            try writer.writeAll("\n");
        }
    }

    try writer.print("\n", .{});
}

fn printStatusJson(
    writer: anytype,
    allocator: Allocator,
    hostname: []const u8,
    metrics: ?SystemMetrics,
    disks: []const DiskInfo,
    top_procs: []const ProcessInfo,
) !void {
    _ = allocator;

    try writer.print("{{\n", .{});
    try jsonStr(writer, "  \"hostname\": ", hostname, ",\n");
    try writer.print("  \"timestamp\": {d},\n", .{std.time.timestamp()});

    if (metrics) |m| {
        try writer.print("  \"cpu_percent\": {d:.2},\n", .{m.cpu_percent});
        try writer.print("  \"cpu_user\": {d:.2},\n", .{m.cpu_user});
        try writer.print("  \"cpu_system\": {d:.2},\n", .{m.cpu_system});
        try writer.print("  \"cpu_iowait\": {d:.2},\n", .{m.cpu_iowait});
        try writer.print("  \"mem_total\": {d},\n", .{m.mem_total});
        try writer.print("  \"mem_used\": {d},\n", .{m.mem_used});
        try writer.print("  \"mem_percent\": {d:.2},\n", .{m.mem_percent});
        try writer.print("  \"swap_total\": {d},\n", .{m.swap_total});
        try writer.print("  \"swap_used\": {d},\n", .{m.swap_used});
    }

    // Disks
    try writer.print("  \"disks\": [\n", .{});
    for (disks, 0..) |disk, i| {
        try writer.print("    {{\n", .{});
        try jsonStr(writer, "      \"mount_point\": ", disk.mount_point, ",\n");
        try jsonStr(writer, "      \"filesystem\": ", disk.filesystem, ",\n");
        try writer.print("      \"total_bytes\": {d},\n", .{disk.total_bytes});
        try writer.print("      \"used_bytes\": {d},\n", .{disk.used_bytes});
        try writer.print("      \"percent\": {d:.2}\n", .{disk.percent});
        try writer.print("    }}{s}\n", .{if (i < disks.len - 1) "," else ""});
    }
    try writer.print("  ],\n", .{});

    // Top processes
    try writer.print("  \"top_processes\": [\n", .{});
    for (top_procs, 0..) |proc, i| {
        try writer.print("    {{\n", .{});
        try writer.print("      \"pid\": {d},\n", .{proc.pid});
        try jsonStr(writer, "      \"name\": ", proc.name, ",\n");
        try writer.print("      \"cpu_percent\": {d:.2},\n", .{proc.cpu_percent});
        try writer.print("      \"mem_rss\": {d},\n", .{proc.mem_rss});
        try writer.print("      \"threads\": {d},\n", .{proc.threads});
        try jsonStr(writer, "      \"username\": ", proc.username, "\n");
        try writer.print("    }}{s}\n", .{if (i < top_procs.len - 1) "," else ""});
    }
    try writer.print("  ]\n", .{});

    try writer.print("}}\n", .{});
}

fn printStatusCsv(
    writer: anytype,
    hostname: []const u8,
    metrics: ?SystemMetrics,
    disks: []const DiskInfo,
    top_procs: []const ProcessInfo,
) !void {
    _ = disks;
    _ = top_procs;

    try writer.print("hostname,timestamp,cpu_percent,cpu_user,cpu_system,cpu_iowait,mem_total,mem_used,mem_percent,swap_total,swap_used\n", .{});

    if (metrics) |m| {
        try writeCsvField(writer, hostname);
        try writer.print(",{d},{d:.2},{d:.2},{d:.2},{d:.2},{d},{d},{d:.2},{d},{d}\n", .{
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
    writer: anytype,
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

fn printMetricsTable(writer: anytype, metrics: []const SystemMetrics) !void {
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

fn printMetricsJson(writer: anytype, metrics: []const SystemMetrics) !void {
    try writer.print("[\n", .{});

    for (metrics, 0..) |m, i| {
        try writer.print("  {{\n", .{});
        try writer.print("    \"cpu_percent\": {d:.2},\n", .{m.cpu_percent});
        try writer.print("    \"cpu_user\": {d:.2},\n", .{m.cpu_user});
        try writer.print("    \"cpu_system\": {d:.2},\n", .{m.cpu_system});
        try writer.print("    \"cpu_iowait\": {d:.2},\n", .{m.cpu_iowait});
        try writer.print("    \"mem_total\": {d},\n", .{m.mem_total});
        try writer.print("    \"mem_used\": {d},\n", .{m.mem_used});
        try writer.print("    \"mem_percent\": {d:.2},\n", .{m.mem_percent});
        try writer.print("    \"swap_total\": {d},\n", .{m.swap_total});
        try writer.print("    \"swap_used\": {d}\n", .{m.swap_used});
        try writer.print("  }}{s}\n", .{if (i < metrics.len - 1) "," else ""});
    }

    try writer.print("]\n", .{});
}

fn printMetricsCsv(writer: anytype, metrics: []const SystemMetrics) !void {
    try writer.print("cpu_percent,cpu_user,cpu_system,cpu_iowait,mem_total,mem_used,mem_percent,swap_total,swap_used\n", .{});

    for (metrics) |m| {
        try writer.print("{d:.2},{d:.2},{d:.2},{d:.2},{d},{d},{d:.2},{d},{d}\n", .{
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
    writer: anytype,
    format: OutputFormat,
    allocator: Allocator,
    processes: []const ProcessInfo,
) !void {
    _ = allocator;

    switch (format) {
        .table => try printProcessesTable(writer, processes),
        .json => try printProcessesJson(writer, processes),
        .csv => try printProcessesCsv(writer, processes),
    }
}

fn printProcessesTable(writer: anytype, processes: []const ProcessInfo) !void {
    try writer.print("\n{s:<8} {s:>7} {s:>9} {s:>6} {s}\n", .{ "PID", "CPU%", "MEM", "STATE", "NAME" });
    try writer.print("{s}\n", .{"-" ** 60});

    for (processes) |proc| {
        var buf: [32]u8 = undefined;
        const mem_str = try formatBytes(proc.mem_rss, &buf);

        try writer.print("{d:<8} {d:6.1}  {s:>9} {c:>6} ", .{
            proc.pid,
            proc.cpu_percent,
            mem_str,
            stateChar(proc.state),
        });
        try writeSafe(writer, proc.name);
        try writer.writeAll("\n");
    }

    try writer.print("\n", .{});
}

fn printProcessesJson(writer: anytype, processes: []const ProcessInfo) !void {
    try writer.print("[\n", .{});

    for (processes, 0..) |proc, i| {
        try writer.print("  {{\n", .{});
        try writer.print("    \"pid\": {d},\n", .{proc.pid});
        try jsonStr(writer, "    \"name\": ", proc.name, ",\n");
        try jsonStr(writer, "    \"cmdline\": ", proc.cmdline, ",\n");
        try jsonStr(writer, "    \"state\": ", &[_]u8{proc.state}, ",\n");
        try writer.print("    \"cpu_percent\": {d:.2},\n", .{proc.cpu_percent});
        try writer.print("    \"mem_rss\": {d},\n", .{proc.mem_rss});
        try writer.print("    \"threads\": {d},\n", .{proc.threads});
        try jsonStr(writer, "    \"username\": ", proc.username, ",\n");
        try writer.print("    \"io_read_bytes\": {d},\n", .{proc.io_read_bytes});
        try writer.print("    \"io_write_bytes\": {d},\n", .{proc.io_write_bytes});
        try jsonStr(writer, "    \"cgroup\": ", proc.cgroup, ",\n");
        try jsonStr(writer, "    \"unit\": ", proc.unit, "\n");
        try writer.print("  }}{s}\n", .{if (i < processes.len - 1) "," else ""});
    }

    try writer.print("]\n", .{});
}

fn printProcessesCsv(writer: anytype, processes: []const ProcessInfo) !void {
    try writer.print("pid,name,cmdline,state,cpu_percent,mem_rss,threads,username,io_read_bytes,io_write_bytes,cgroup,unit\n", .{});

    for (processes) |proc| {
        try writer.print("{d},", .{proc.pid});
        try writeCsvField(writer, proc.name);
        try writer.writeByte(',');
        try writeCsvField(writer, proc.cmdline);
        try writer.writeByte(',');
        try writeCsvField(writer, &[_]u8{proc.state});
        try writer.print(",{d:.2},{d},{d},", .{ proc.cpu_percent, proc.mem_rss, proc.threads });
        try writeCsvField(writer, proc.username);
        try writer.print(",{d},{d},", .{ proc.io_read_bytes, proc.io_write_bytes });
        try writeCsvField(writer, proc.cgroup);
        try writer.writeByte(',');
        try writeCsvField(writer, proc.unit);
        try writer.writeByte('\n');
    }
}

// Log entries output
pub fn printLogs(
    writer: anytype,
    format: OutputFormat,
    allocator: Allocator,
    logs: []const LogEntry,
) !void {
    _ = allocator;

    switch (format) {
        .table => try printLogsTable(writer, logs),
        .json => try printLogsJson(writer, logs),
        .csv => try printLogsCsv(writer, logs),
    }
}

fn printLogsTable(writer: anytype, logs: []const LogEntry) !void {
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

        try writer.print("{d:02}:{d:02}:{d:02} [", .{ h, min, sec });
        try writeSafe(writer, unit_str);
        try writer.print("] {s}: ", .{priority_str});
        try writeSafe(writer, log.message);
        try writer.writeAll("\n");
    }

    try writer.print("\n", .{});
}

fn printLogsJson(writer: anytype, logs: []const LogEntry) !void {
    try writer.print("[\n", .{});

    for (logs, 0..) |log, i| {
        try writer.print("  {{\n", .{});
        try writer.print("    \"timestamp\": {d},\n", .{log.timestamp});
        try jsonStr(writer, "    \"source\": ", log.source, ",\n");

        if (log.unit) |unit| {
            try jsonStr(writer, "    \"unit\": ", unit, ",\n");
        } else {
            try writer.print("    \"unit\": null,\n", .{});
        }

        if (log.identifier) |identifier| {
            try jsonStr(writer, "    \"identifier\": ", identifier, ",\n");
        } else {
            try writer.print("    \"identifier\": null,\n", .{});
        }

        if (log.systemd_unit) |systemd_unit| {
            try jsonStr(writer, "    \"systemd_unit\": ", systemd_unit, ",\n");
        } else {
            try writer.print("    \"systemd_unit\": null,\n", .{});
        }

        try writer.print("    \"priority\": {d},\n", .{log.priority});
        try jsonStr(writer, "    \"message\": ", log.message, "");

        if (log.pid) |pid| {
            try writer.print(",\n    \"pid\": {d}\n", .{pid});
        } else {
            try writer.print("\n", .{});
        }

        try writer.print("  }}{s}\n", .{if (i < logs.len - 1) "," else ""});
    }

    try writer.print("]\n", .{});
}

fn printLogsCsv(writer: anytype, logs: []const LogEntry) !void {
    try writer.print("timestamp,source,unit,identifier,systemd_unit,priority,message,pid\n", .{});

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
        try writer.print(",{d}\n", .{pid_str});
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
    writer: anytype,
    format: OutputFormat,
    allocator: Allocator,
    columns: []const []const u8,
    rows: []const []?[]const u8,
) !void {
    _ = allocator;

    switch (format) {
        .table => try printQueryTable(writer, columns, rows),
        .json => try printQueryJson(writer, columns, rows),
        .csv => try printQueryCsv(writer, columns, rows),
    }
}

fn printQueryTable(writer: anytype, columns: []const []const u8, rows: []const []?[]const u8) !void {
    try writer.print("\n", .{});

    for (columns) |col| {
        try writeCell(writer, col, 20, false);
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
            try writeCell(writer, val, 20, false);
            try writer.writeByte(' ');
        }
        try writer.print("\n", .{});
    }

    try writer.print("\n", .{});
}

fn printQueryJson(writer: anytype, columns: []const []const u8, rows: []const []?[]const u8) !void {
    try writer.print("[\n", .{});

    for (rows, 0..) |row, i| {
        try writer.print("  {{\n", .{});

        for (columns, row, 0..) |col, cell, j| {
            if (cell) |val| {
                try writer.writeAll("    ");
                try writeJsonString(writer, col);
                try jsonStr(writer, ": ", val, if (j < columns.len - 1) ",\n" else "\n");
            } else {
                try writer.writeAll("    ");
                try writeJsonString(writer, col);
                try writer.writeAll(if (j < columns.len - 1) ": null,\n" else ": null\n");
            }
        }

        try writer.print("  }}{s}\n", .{if (i < rows.len - 1) "," else ""});
    }

    try writer.print("]\n", .{});
}

fn printQueryCsv(writer: anytype, columns: []const []const u8, rows: []const []?[]const u8) !void {
    for (columns, 0..) |col, i| {
        try writeCsvField(writer, col);
        try writer.writeAll(if (i < columns.len - 1) "," else "\n");
    }

    for (rows) |row| {
        for (row, 0..) |cell, i| {
            const val = cell orelse "";
            try writeCsvField(writer, val);
            try writer.writeAll(if (i < row.len - 1) "," else "\n");
        }
    }
}

// ── Tests ──

const testing = std.testing;

fn testProc(name: []const u8, cmdline: []const u8) ProcessInfo {
    return .{
        .pid = 42,
        .name = name,
        .cmdline = cmdline,
        .state = 'S',
        .cpu_percent = 1.5,
        .mem_rss = 2048,
        .threads = 3,
        .username = "user",
        .io_read_bytes = 1,
        .io_write_bytes = 2,
        .cgroup = "/a",
        .unit = "u",
    };
}

fn testLog(message: []const u8) LogEntry {
    return .{
        .timestamp = 3600,
        .source = "systemd",
        .unit = "unit",
        .identifier = "id",
        .systemd_unit = null,
        .priority = 6,
        .message = message,
        .pid = 7,
    };
}

/// Minimal RFC 4180 reader: returns records as lists of fields.
fn parseCsv(a: Allocator, text: []const u8) ![][][]u8 {
    var records: std.ArrayList([][]u8) = .empty;
    var fields: std.ArrayList([]u8) = .empty;
    var field: std.ArrayList(u8) = .empty;
    var i: usize = 0;
    var in_quotes = false;
    while (i < text.len) : (i += 1) {
        const c = text[i];
        if (in_quotes) {
            if (c == '"') {
                if (i + 1 < text.len and text[i + 1] == '"') {
                    try field.append(a, '"');
                    i += 1;
                } else in_quotes = false;
            } else try field.append(a, c);
        } else switch (c) {
            '"' => in_quotes = true,
            ',' => {
                try fields.append(a, try field.toOwnedSlice(a));
            },
            '\r' => {},
            '\n' => {
                try fields.append(a, try field.toOwnedSlice(a));
                try records.append(a, try fields.toOwnedSlice(a));
            },
            else => try field.append(a, c),
        }
    }
    try testing.expect(!in_quotes);
    return records.toOwnedSlice(a);
}

const hostile = ",X\r\n9999,\"forged";

test "table: process name control bytes are escaped and columns stay aligned" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var list: std.ArrayList(u8) = .empty;
    const procs = [_]ProcessInfo{ testProc("\x1b[1A\x1b[2K\rfake", "c"), testProc("ok", "c") };
    try printProcesses(list.writer(a), .table, a, &procs);
    for (list.items) |c| try testing.expect(c == '\n' or (c >= 0x20 and c != 0x7f));
    try testing.expect(std.mem.indexOf(u8, list.items, "\\x1b[1A\\x1b[2K\\x0dfake") != null);
    var it = std.mem.splitScalar(u8, list.items, '\n');
    var cols: [2]usize = undefined;
    var n: usize = 0;
    while (it.next()) |line| {
        if (std.mem.startsWith(u8, line, "42 ")) {
            cols[n] = std.mem.indexOf(u8, line, "S ").? + 2;
            n += 1;
        }
    }
    try testing.expectEqual(@as(usize, 2), n);
    try testing.expectEqual(cols[0], cols[1]);
}

test "table: query cells and log fields are escaped and padded after sanitizing" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var list: std.ArrayList(u8) = .empty;
    const cols = [_][]const u8{"c\x1b"};
    var row = [_]?[]const u8{"\x1b[2J"};
    const rows = [_][]?[]const u8{&row};
    try printQueryResult(list.writer(a), .table, a, &cols, &rows);
    var logs = [_]LogEntry{testLog("m\x1b[2J\r")};
    try printLogs(list.writer(a), .table, a, &logs);
    for (list.items) |c| try testing.expect(c == '\n' or (c >= 0x20 and c != 0x7f));
    try testing.expect(std.mem.indexOf(u8, list.items, "\\x1b[2J" ++ " " ** 14 ++ "\n") != null);
}

test "table: status hostname and mount point are escaped" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var list: std.ArrayList(u8) = .empty;
    const disks = [_]DiskInfo{.{ .mount_point = "/\x1b[2J", .filesystem = "x", .total_bytes = 10, .used_bytes = 5, .percent = 50 }};
    try printStatus(list.writer(a), .table, a, "h\r\x1b", null, &disks, &.{});
    for (list.items) |c| try testing.expect(c == '\n' or (c >= 0x20 and c != 0x7f));
}

test "json: process, log and query output round-trips hostile strings" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const evil = "q\"b\\s\nn\r\t\x01\x1b\x7f end";

    var list: std.ArrayList(u8) = .empty;
    var proc = testProc(evil, evil);
    proc.username = evil;
    proc.cgroup = evil;
    proc.unit = evil;
    proc.state = 0x01;
    const procs = [_]ProcessInfo{proc};
    try printProcesses(list.writer(a), .json, a, &procs);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, list.items, .{});
    const obj = parsed.value.array.items[0].object;
    try testing.expectEqualStrings(evil, obj.get("name").?.string);
    try testing.expectEqualStrings(evil, obj.get("cmdline").?.string);
    try testing.expectEqualStrings(evil, obj.get("username").?.string);
    try testing.expectEqualStrings(evil, obj.get("cgroup").?.string);
    try testing.expectEqualStrings(evil, obj.get("unit").?.string);
    try testing.expectEqualStrings("\x01", obj.get("state").?.string);

    list.clearRetainingCapacity();
    var logs = [_]LogEntry{testLog(evil)};
    logs[0].unit = evil;
    logs[0].systemd_unit = evil;
    logs[0].source = evil;
    logs[0].identifier = evil;
    try printLogs(list.writer(a), .json, a, &logs);
    parsed = try std.json.parseFromSlice(std.json.Value, a, list.items, .{});
    const lobj = parsed.value.array.items[0].object;
    try testing.expectEqualStrings(evil, lobj.get("message").?.string);
    try testing.expectEqualStrings(evil, lobj.get("unit").?.string);
    try testing.expectEqualStrings(evil, lobj.get("systemd_unit").?.string);
    try testing.expectEqualStrings(evil, lobj.get("source").?.string);
    try testing.expectEqualStrings(evil, lobj.get("identifier").?.string);

    list.clearRetainingCapacity();
    const cols = [_][]const u8{ evil, "b" };
    var row = [_]?[]const u8{ evil, null };
    const rows = [_][]?[]const u8{&row};
    try printQueryResult(list.writer(a), .json, a, &cols, &rows);
    parsed = try std.json.parseFromSlice(std.json.Value, a, list.items, .{});
    try testing.expectEqualStrings(evil, parsed.value.array.items[0].object.get(evil).?.string);

    list.clearRetainingCapacity();
    const disks = [_]DiskInfo{.{ .mount_point = evil, .filesystem = evil, .total_bytes = 10, .used_bytes = 5, .percent = 50 }};
    try printStatus(list.writer(a), .json, a, evil, null, &disks, &procs);
    parsed = try std.json.parseFromSlice(std.json.Value, a, list.items, .{});
    const sobj = parsed.value.object;
    try testing.expectEqualStrings(evil, sobj.get("hostname").?.string);
    const dobj = sobj.get("disks").?.array.items[0].object;
    try testing.expectEqualStrings(evil, dobj.get("mount_point").?.string);
    try testing.expectEqualStrings(evil, dobj.get("filesystem").?.string);
}

test "json: invalid UTF-8 becomes U+FFFD and valid multibyte is preserved" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var list: std.ArrayList(u8) = .empty;
    const procs = [_]ProcessInfo{testProc("a\x9b2Jb\xc3", "\xe6\x97\xa5\xe6\x9c\xac na\xc3\xafve")};
    try printProcesses(list.writer(a), .json, a, &procs);
    try testing.expect(std.unicode.utf8ValidateSlice(list.items));
    const parsed = try std.json.parseFromSlice(std.json.Value, a, list.items, .{});
    const obj = parsed.value.array.items[0].object;
    try testing.expectEqualStrings("a\xef\xbf\xbd2Jb\xef\xbf\xbd", obj.get("name").?.string);
    try testing.expectEqualStrings("\xe6\x97\xa5\xe6\x9c\xac na\xc3\xafve", obj.get("cmdline").?.string);
}

test "table: C1 controls and invalid UTF-8 are escaped, printable UTF-8 kept" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var list: std.ArrayList(u8) = .empty;
    var proc = testProc("a\xc2\x9b2J \x9b \xe6\x97\xa5 na\xc3\xafve", "c");
    proc.state = 0x9b;
    const procs = [_]ProcessInfo{ proc, testProc("x\x01", "c") };
    try printProcesses(list.writer(a), .table, a, &procs);
    try testing.expect(std.mem.indexOf(u8, list.items, "\xc2\x9b") == null);
    try testing.expect(std.mem.indexOfScalar(u8, list.items, 0x9b) == null);
    try testing.expect(std.mem.indexOf(u8, list.items, "a\\xc2\\x9b2J \\x9b \xe6\x97\xa5 na\xc3\xafve") != null);
    try testing.expect(std.mem.indexOf(u8, list.items, "      ? a\\xc2") != null);
}

test "csv: process cmdline cannot forge records" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var list: std.ArrayList(u8) = .empty;
    const cmd = "\"" ++ hostile;
    const procs = [_]ProcessInfo{testProc("n\"ame", cmd)};
    try printProcesses(list.writer(a), .csv, a, &procs);
    const recs = try parseCsv(a, list.items);
    try testing.expectEqual(@as(usize, 2), recs.len);
    try testing.expectEqual(recs[0].len, recs[1].len);
    try testing.expectEqualStrings(cmd, recs[1][2]);
    try testing.expectEqualStrings("n\"ame", recs[1][1]);
}

test "csv: log message cannot forge records" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var list: std.ArrayList(u8) = .empty;
    const msg = "\"" ++ hostile;
    var logs = [_]LogEntry{testLog(msg)};
    logs[0].unit = msg;
    try printLogs(list.writer(a), .csv, a, &logs);
    const recs = try parseCsv(a, list.items);
    try testing.expectEqual(@as(usize, 2), recs.len);
    try testing.expectEqual(recs[0].len, recs[1].len);
    try testing.expectEqualStrings(msg, recs[1][6]);
    try testing.expectEqualStrings(msg, recs[1][2]);
}

test "csv: query cells and status hostname cannot forge records" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var list: std.ArrayList(u8) = .empty;
    const cell = "\"" ++ hostile;
    const cols = [_][]const u8{ "a\"", "b" };
    var row = [_]?[]const u8{ cell, null };
    const rows = [_][]?[]const u8{&row};
    try printQueryResult(list.writer(a), .csv, a, &cols, &rows);
    var recs = try parseCsv(a, list.items);
    try testing.expectEqual(@as(usize, 2), recs.len);
    try testing.expectEqualStrings(cell, recs[1][0]);
    try testing.expectEqualStrings("a\"", recs[0][0]);

    list.clearRetainingCapacity();
    const m = std.mem.zeroes(SystemMetrics);
    try printStatus(list.writer(a), .csv, a, cell, m, &.{}, &.{});
    recs = try parseCsv(a, list.items);
    try testing.expectEqual(@as(usize, 2), recs.len);
    try testing.expectEqual(recs[0].len, recs[1].len);
    try testing.expectEqualStrings(cell, recs[1][0]);
}
