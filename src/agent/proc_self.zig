//! Reads the daemon's own resource usage from /proc/self.
//!
//! The daemon is supposed to be lightweight (~0.1% CPU, ~50 MB RSS on a
//! fast SSD). Pushing these self-metrics with each cycle lets the hosted
//! side alert when a daemon drifts up - long-running state, leaks, or a
//! regression in our own collection code.

const std = @import("std");

pub const Sample = struct {
    cpu_percent: f32,
    rss_kb: u64,
    vsize_kb: u64,
    threads: u32,
    voluntary_ctxt_switches: u64,
    nonvoluntary_ctxt_switches: u64,
    uptime_seconds: u64,
};

/// Per-daemon state needed to compute a CPU-percent delta between cycles.
pub const State = struct {
    last_cpu_jiffies: ?u64 = null,
    last_sample_ns: ?i128 = null,
    started_ns: i128,

    pub fn init() State {
        return .{ .started_ns = std.time.nanoTimestamp() };
    }
};

/// Sample /proc/self/{stat,status} once. Returns a Sample with cpu_percent
/// computed against the last call's CPU jiffies; first call returns 0%.
pub fn sample(state: *State) !Sample {
    const stat = try readStat();
    const status = try readStatus();

    const now_ns = std.time.nanoTimestamp();
    const cpu_pct: f32 = blk: {
        const last_j = state.last_cpu_jiffies orelse {
            state.last_cpu_jiffies = stat.cpu_jiffies;
            state.last_sample_ns = now_ns;
            break :blk 0.0;
        };
        const last_ns = state.last_sample_ns orelse {
            state.last_cpu_jiffies = stat.cpu_jiffies;
            state.last_sample_ns = now_ns;
            break :blk 0.0;
        };
        const dt_ns = now_ns - last_ns;
        if (dt_ns <= 0) break :blk 0.0;

        const dj = if (stat.cpu_jiffies >= last_j) stat.cpu_jiffies - last_j else 0;
        // CLK_TCK is 100 on every modern Linux kernel; jiffies-to-seconds
        // requires it to be exact. Hardcoding avoids a libc dep here.
        const clk_tck: f64 = 100.0;
        const cpu_seconds: f64 = @as(f64, @floatFromInt(dj)) / clk_tck;
        const wall_seconds: f64 = @as(f64, @floatFromInt(dt_ns)) / std.time.ns_per_s;
        state.last_cpu_jiffies = stat.cpu_jiffies;
        state.last_sample_ns = now_ns;
        break :blk @floatCast(cpu_seconds / wall_seconds * 100.0);
    };

    const uptime_ns = now_ns - state.started_ns;
    const uptime_s: u64 = @intCast(@divFloor(uptime_ns, std.time.ns_per_s));

    return .{
        .cpu_percent = cpu_pct,
        .rss_kb = status.vm_rss_kb,
        .vsize_kb = status.vm_size_kb,
        .threads = status.threads,
        .voluntary_ctxt_switches = status.voluntary_ctxt_switches,
        .nonvoluntary_ctxt_switches = status.nonvoluntary_ctxt_switches,
        .uptime_seconds = uptime_s,
    };
}

const StatRead = struct {
    cpu_jiffies: u64,
};

fn readStat() !StatRead {
    var buf: [4096]u8 = undefined;
    const n = try readAll("/proc/self/stat", &buf);
    const slice = buf[0..n];

    // After comm field (in parens) the rest is space-separated.
    // utime is field 14, stime is field 15 (1-indexed).
    const close_paren = std.mem.lastIndexOfScalar(u8, slice, ')') orelse return error.MalformedProcStat;
    var it = std.mem.tokenizeScalar(u8, slice[close_paren + 1 ..], ' ');

    var idx: usize = 2; // we start counting after pid + comm
    var utime: u64 = 0;
    var stime: u64 = 0;
    while (it.next()) |tok| : (idx += 1) {
        if (idx == 14) utime = std.fmt.parseInt(u64, tok, 10) catch 0;
        if (idx == 15) {
            stime = std.fmt.parseInt(u64, tok, 10) catch 0;
            break;
        }
    }
    return .{ .cpu_jiffies = utime + stime };
}

const StatusRead = struct {
    vm_rss_kb: u64 = 0,
    vm_size_kb: u64 = 0,
    threads: u32 = 0,
    voluntary_ctxt_switches: u64 = 0,
    nonvoluntary_ctxt_switches: u64 = 0,
};

fn readStatus() !StatusRead {
    var buf: [8192]u8 = undefined;
    const n = try readAll("/proc/self/status", &buf);
    var out: StatusRead = .{};
    var lines = std.mem.splitScalar(u8, buf[0..n], '\n');
    while (lines.next()) |line| {
        out.vm_rss_kb = parseKb(line, "VmRSS:") orelse out.vm_rss_kb;
        out.vm_size_kb = parseKb(line, "VmSize:") orelse out.vm_size_kb;
        if (extractInt(u32, line, "Threads:")) |v| out.threads = v;
        if (extractInt(u64, line, "voluntary_ctxt_switches:")) |v| out.voluntary_ctxt_switches = v;
        if (extractInt(u64, line, "nonvoluntary_ctxt_switches:")) |v| out.nonvoluntary_ctxt_switches = v;
    }
    return out;
}

fn parseKb(line: []const u8, prefix: []const u8) ?u64 {
    if (!std.mem.startsWith(u8, line, prefix)) return null;
    var it = std.mem.tokenizeAny(u8, line[prefix.len..], " \t\n\r");
    const tok = it.next() orelse return null;
    return std.fmt.parseInt(u64, tok, 10) catch null;
}

fn extractInt(comptime T: type, line: []const u8, prefix: []const u8) ?T {
    if (!std.mem.startsWith(u8, line, prefix)) return null;
    var it = std.mem.tokenizeAny(u8, line[prefix.len..], " \t\n\r");
    const tok = it.next() orelse return null;
    return std.fmt.parseInt(T, tok, 10) catch null;
}

fn readAll(path: []const u8, dest: []u8) !usize {
    const file = try std.fs.openFileAbsolute(path, .{});
    defer file.close();
    return file.readAll(dest);
}

test "extractInt parses Threads line" {
    try std.testing.expectEqual(@as(?u32, 3), extractInt(u32, "Threads:\t3\n", "Threads:"));
    try std.testing.expectEqual(@as(?u32, null), extractInt(u32, "Other: 5\n", "Threads:"));
}

test "parseKb parses VmRSS" {
    try std.testing.expectEqual(@as(?u64, 12340), parseKb("VmRSS:\t12340 kB\n", "VmRSS:"));
}
