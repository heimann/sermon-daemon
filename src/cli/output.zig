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

    try writer.print("\nSERMON STATUS - {s}\n", .{hostname});
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
            try writer.print("  {s:<12} {d:5.1}%  ({s} / {s})\n", .{
                disk.mount_point,
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
            try writer.print("  {d:<8} {d:5.1}%  {s:<9} {s}\n", .{
                proc.pid,
                proc.cpu_percent,
                mem_str,
                proc.name,
            });
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
    _ = allocator;

    try writer.print("{{\n", .{});
    try writer.print("  \"hostname\": \"{s}\",\n", .{hostname});
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
        try writer.print("      \"mount_point\": \"{s}\",\n", .{disk.mount_point});
        try writer.print("      \"filesystem\": \"{s}\",\n", .{disk.filesystem});
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
        try writer.print("      \"name\": \"{s}\",\n", .{proc.name});
        try writer.print("      \"cpu_percent\": {d:.2},\n", .{proc.cpu_percent});
        try writer.print("      \"mem_rss\": {d},\n", .{proc.mem_rss});
        try writer.print("      \"threads\": {d},\n", .{proc.threads});
        try writer.print("      \"username\": \"{s}\"\n", .{proc.username});
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

    try writer.print("hostname,timestamp,cpu_percent,cpu_user,cpu_system,cpu_iowait,mem_total,mem_used,mem_percent,swap_total,swap_used\n", .{});

    if (metrics) |m| {
        try writer.print("{s},{d},{d:.2},{d:.2},{d:.2},{d:.2},{d},{d},{d:.2},{d},{d}\n", .{
            hostname,
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

fn printMetricsCsv(writer: Writer, metrics: []const SystemMetrics) !void {
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
    writer: Writer,
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

fn printProcessesTable(writer: Writer, processes: []const ProcessInfo) !void {
    try writer.print("\n{s:<8} {s:>7} {s:>9} {s:>6} {s}\n", .{ "PID", "CPU%", "MEM", "STATE", "NAME" });
    try writer.print("{s}\n", .{"-" ** 60});

    for (processes) |proc| {
        var buf: [32]u8 = undefined;
        const mem_str = try formatBytes(proc.mem_rss, &buf);

        try writer.print("{d:<8} {d:6.1}  {s:>9} {c:>6} {s}\n", .{
            proc.pid,
            proc.cpu_percent,
            mem_str,
            proc.state,
            proc.name,
        });
    }

    try writer.print("\n", .{});
}

fn printProcessesJson(writer: Writer, processes: []const ProcessInfo) !void {
    try writer.print("[\n", .{});

    for (processes, 0..) |proc, i| {
        try writer.print("  {{\n", .{});
        try writer.print("    \"pid\": {d},\n", .{proc.pid});
        try writer.print("    \"name\": \"{s}\",\n", .{proc.name});
        try writer.print("    \"cmdline\": \"{s}\",\n", .{proc.cmdline});
        try writer.print("    \"state\": \"{c}\",\n", .{proc.state});
        try writer.print("    \"cpu_percent\": {d:.2},\n", .{proc.cpu_percent});
        try writer.print("    \"mem_rss\": {d},\n", .{proc.mem_rss});
        try writer.print("    \"threads\": {d},\n", .{proc.threads});
        try writer.print("    \"username\": \"{s}\"\n", .{proc.username});
        try writer.print("  }}{s}\n", .{if (i < processes.len - 1) "," else ""});
    }

    try writer.print("]\n", .{});
}

fn printProcessesCsv(writer: Writer, processes: []const ProcessInfo) !void {
    try writer.print("pid,name,cmdline,state,cpu_percent,mem_rss,threads,username\n", .{});

    for (processes) |proc| {
        // Escape cmdline for CSV (replace quotes with double quotes)
        try writer.print("{d},\"{s}\",\"{s}\",{c},{d:.2},{d},{d},\"{s}\"\n", .{
            proc.pid,
            proc.name,
            proc.cmdline,
            proc.state,
            proc.cpu_percent,
            proc.mem_rss,
            proc.threads,
            proc.username,
        });
    }
}

// Log entries output
pub fn printLogs(
    writer: Writer,
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

        try writer.print("{d:02}:{d:02}:{d:02} [{s}] {s}: {s}\n", .{
            h,
            min,
            sec,
            unit_str,
            priority_str,
            log.message,
        });
    }

    try writer.print("\n", .{});
}

fn printLogsJson(writer: Writer, logs: []const LogEntry) !void {
    try writer.print("[\n", .{});

    for (logs, 0..) |log, i| {
        try writer.print("  {{\n", .{});
        try writer.print("    \"timestamp\": {d},\n", .{log.timestamp});
        try writer.print("    \"source\": \"{s}\",\n", .{log.source});

        if (log.unit) |unit| {
            try writer.print("    \"unit\": \"{s}\",\n", .{unit});
        } else {
            try writer.print("    \"unit\": null,\n", .{});
        }

        try writer.print("    \"priority\": {d},\n", .{log.priority});
        try writer.print("    \"message\": \"{s}\"", .{log.message});

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
    try writer.print("timestamp,source,unit,priority,message,pid\n", .{});

    for (logs) |log| {
        const unit_str = log.unit orelse "";
        const pid_str = if (log.pid) |pid| pid else 0;

        try writer.print("{d},\"{s}\",\"{s}\",{d},\"{s}\",{d}\n", .{
            log.timestamp,
            log.source,
            unit_str,
            log.priority,
            log.message,
            pid_str,
        });
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
    _ = allocator;

    switch (format) {
        .table => try printQueryTable(writer, columns, rows),
        .json => try printQueryJson(writer, columns, rows),
        .csv => try printQueryCsv(writer, columns, rows),
    }
}

fn printQueryTable(writer: Writer, columns: []const []const u8, rows: []const []?[]const u8) !void {
    try writer.print("\n", .{});

    for (columns) |col| {
        try writer.print("{s:<20} ", .{col});
    }
    try writer.print("\n", .{});

    for (columns) |_| {
        try writer.print("{s} ", .{"-" ** 20});
    }
    try writer.print("\n", .{});

    for (rows) |row| {
        for (row) |cell| {
            const val = cell orelse "NULL";
            try writer.print("{s:<20} ", .{val});
        }
        try writer.print("\n", .{});
    }

    try writer.print("\n", .{});
}

fn printQueryJson(writer: Writer, columns: []const []const u8, rows: []const []?[]const u8) !void {
    try writer.print("[\n", .{});

    for (rows, 0..) |row, i| {
        try writer.print("  {{\n", .{});

        for (columns, row, 0..) |col, cell, j| {
            if (cell) |val| {
                try writer.print("    \"{s}\": \"{s}\"{s}\n", .{
                    col,
                    val,
                    if (j < columns.len - 1) "," else "",
                });
            } else {
                try writer.print("    \"{s}\": null{s}\n", .{
                    col,
                    if (j < columns.len - 1) "," else "",
                });
            }
        }

        try writer.print("  }}{s}\n", .{if (i < rows.len - 1) "," else ""});
    }

    try writer.print("]\n", .{});
}

fn printQueryCsv(writer: Writer, columns: []const []const u8, rows: []const []?[]const u8) !void {
    for (columns, 0..) |col, i| {
        try writer.print("{s}{s}", .{ col, if (i < columns.len - 1) "," else "\n" });
    }

    for (rows) |row| {
        for (row, 0..) |cell, i| {
            const val = cell orelse "";
            try writer.print("\"{s}\"{s}", .{ val, if (i < row.len - 1) "," else "\n" });
        }
    }
}
