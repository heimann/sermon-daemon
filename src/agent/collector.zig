const std = @import("std");
const Allocator = std.mem.Allocator;
const fs = std.fs;
const os = std.os;

// ============================================================================
// Public Data Types (Interface Contract)
// ============================================================================

pub const SystemMetrics = struct {
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

pub const ProcessInfo = struct {
    pid: u32,
    name: []const u8,
    cmdline: []const u8,
    state: u8,
    cpu_percent: f32,
    mem_rss: u64,
    threads: u32,
    username: []const u8,
};

pub const DiskInfo = struct {
    mount_point: []const u8,
    filesystem: []const u8,
    total_bytes: u64,
    used_bytes: u64,
    percent: f32,
};

// ============================================================================
// Internal State
// ============================================================================

const CpuStats = struct {
    user: u64,
    nice: u64,
    system: u64,
    idle: u64,
    iowait: u64,
    irq: u64,
    softirq: u64,
    steal: u64,

    fn total(self: CpuStats) u64 {
        return self.user + self.nice + self.system + self.idle +
            self.iowait + self.irq + self.softirq + self.steal;
    }
};

const ProcessStats = struct {
    utime: u64,
    stime: u64,
    timestamp: i64,
};

pub const Collector = struct {
    allocator: Allocator,
    prev_cpu: ?CpuStats,
    prev_processes: std.AutoHashMap(u32, ProcessStats),
    clock_ticks: u64,
    page_size: u64,

    pub fn init(allocator: Allocator) !Collector {
        const clock_ticks = readClockTicks() catch 100;

        return Collector{
            .allocator = allocator,
            .prev_cpu = null,
            .prev_processes = std.AutoHashMap(u32, ProcessStats).init(allocator),
            .clock_ticks = clock_ticks,
            .page_size = @intCast(std.c.sysconf(@intFromEnum(std.c._SC.PAGESIZE))),
        };
    }

    fn readClockTicks() !u64 {
        // Try reading from getconf CLK_TCK via /proc/self/auxv or use standard value
        // For simplicity, we'll use the standard Linux value of 100
        return 100;
    }

    pub fn deinit(self: *Collector) void {
        self.prev_processes.deinit();
    }

    // ========================================================================
    // CPU Metrics Collection
    // ========================================================================

    pub fn collectMetrics(self: *Collector) !SystemMetrics {
        const cpu_stats = try self.readCpuStats();
        const mem_stats = try self.readMemStats();

        // Calculate CPU percentages
        var cpu_percent: f32 = 0.0;
        var cpu_user: f32 = 0.0;
        var cpu_system: f32 = 0.0;
        var cpu_iowait: f32 = 0.0;

        if (self.prev_cpu) |prev| {
            const total_delta = cpu_stats.total() - prev.total();
            if (total_delta > 0) {
                const idle_delta = cpu_stats.idle - prev.idle;
                cpu_percent = 100.0 * @as(f32, @floatFromInt(total_delta - idle_delta)) / @as(f32, @floatFromInt(total_delta));
                cpu_user = 100.0 * @as(f32, @floatFromInt((cpu_stats.user + cpu_stats.nice) - (prev.user + prev.nice))) / @as(f32, @floatFromInt(total_delta));
                cpu_system = 100.0 * @as(f32, @floatFromInt(cpu_stats.system - prev.system)) / @as(f32, @floatFromInt(total_delta));
                cpu_iowait = 100.0 * @as(f32, @floatFromInt(cpu_stats.iowait - prev.iowait)) / @as(f32, @floatFromInt(total_delta));
            }
        }

        self.prev_cpu = cpu_stats;

        return SystemMetrics{
            .cpu_percent = cpu_percent,
            .cpu_user = cpu_user,
            .cpu_system = cpu_system,
            .cpu_iowait = cpu_iowait,
            .mem_total = mem_stats.total,
            .mem_used = mem_stats.used,
            .mem_percent = mem_stats.percent,
            .swap_total = mem_stats.swap_total,
            .swap_used = mem_stats.swap_used,
        };
    }

    fn readCpuStats(self: *Collector) !CpuStats {
        _ = self;
        const file = try fs.openFileAbsolute("/proc/stat", .{});
        defer file.close();

        var buf: [4096]u8 = undefined;
        const bytes_read = try file.readAll(&buf);
        const content = buf[0..bytes_read];

        const line_end = std.mem.indexOfScalar(u8, content, '\n') orelse return error.InvalidProcStat;
        const line = content[0..line_end];

        // Parse first line: cpu  user nice system idle iowait irq softirq steal
        var it = std.mem.tokenizeScalar(u8, line, ' ');

        const cpu_label = it.next() orelse return error.InvalidProcStat;
        if (!std.mem.eql(u8, cpu_label, "cpu")) return error.InvalidProcStat;

        const user = try std.fmt.parseInt(u64, it.next() orelse return error.InvalidProcStat, 10);
        const nice = try std.fmt.parseInt(u64, it.next() orelse return error.InvalidProcStat, 10);
        const system = try std.fmt.parseInt(u64, it.next() orelse return error.InvalidProcStat, 10);
        const idle = try std.fmt.parseInt(u64, it.next() orelse return error.InvalidProcStat, 10);
        const iowait = try std.fmt.parseInt(u64, it.next() orelse return error.InvalidProcStat, 10);
        const irq = try std.fmt.parseInt(u64, it.next() orelse return error.InvalidProcStat, 10);
        const softirq = try std.fmt.parseInt(u64, it.next() orelse return error.InvalidProcStat, 10);
        const steal = if (it.next()) |s| try std.fmt.parseInt(u64, s, 10) else 0;

        return CpuStats{
            .user = user,
            .nice = nice,
            .system = system,
            .idle = idle,
            .iowait = iowait,
            .irq = irq,
            .softirq = softirq,
            .steal = steal,
        };
    }

    const MemStats = struct {
        total: u64,
        used: u64,
        percent: f32,
        swap_total: u64,
        swap_used: u64,
    };

    fn readMemStats(self: *Collector) !MemStats {
        _ = self;
        const file = try fs.openFileAbsolute("/proc/meminfo", .{});
        defer file.close();

        var buf: [4096]u8 = undefined;
        const bytes_read = try file.readAll(&buf);
        const content = buf[0..bytes_read];

        var mem_total: u64 = 0;
        var mem_available: u64 = 0;
        var swap_total: u64 = 0;
        var swap_free: u64 = 0;

        var it = std.mem.splitScalar(u8, content, '\n');
        while (it.next()) |line| {
            var token_it = std.mem.tokenizeAny(u8, line, ": ");
            const key = token_it.next() orelse continue;
            const value_str = token_it.next() orelse continue;
            const value = std.fmt.parseInt(u64, value_str, 10) catch continue;

            if (std.mem.eql(u8, key, "MemTotal")) {
                mem_total = value * 1024; // Convert KB to bytes
            } else if (std.mem.eql(u8, key, "MemAvailable")) {
                mem_available = value * 1024;
            } else if (std.mem.eql(u8, key, "SwapTotal")) {
                swap_total = value * 1024;
            } else if (std.mem.eql(u8, key, "SwapFree")) {
                swap_free = value * 1024;
            }
        }

        const mem_used = if (mem_total > mem_available) mem_total - mem_available else 0;
        const mem_percent = if (mem_total > 0)
            100.0 * @as(f32, @floatFromInt(mem_used)) / @as(f32, @floatFromInt(mem_total))
        else
            0.0;
        const swap_used = if (swap_total > swap_free) swap_total - swap_free else 0;

        return MemStats{
            .total = mem_total,
            .used = mem_used,
            .percent = mem_percent,
            .swap_total = swap_total,
            .swap_used = swap_used,
        };
    }

    // ========================================================================
    // Process Collection
    // ========================================================================

    pub fn collectProcesses(self: *Collector, allocator: Allocator) ![]ProcessInfo {
        var processes = try std.ArrayList(ProcessInfo).initCapacity(allocator, 128);
        errdefer {
            for (processes.items) |proc| {
                allocator.free(proc.name);
                allocator.free(proc.cmdline);
                allocator.free(proc.username);
            }
            processes.deinit(allocator);
        }

        const current_time = std.time.timestamp();

        var proc_dir = try fs.openDirAbsolute("/proc", .{ .iterate = true });
        defer proc_dir.close();

        var it = proc_dir.iterate();
        while (try it.next()) |entry| {
            if (entry.kind != .directory) continue;

            const pid = std.fmt.parseInt(u32, entry.name, 10) catch continue;

            if (self.collectProcess(allocator, pid, current_time)) |proc_info| {
                try processes.append(allocator, proc_info);
            } else |_| {
                // Process may have exited, skip it
                continue;
            }
        }

        return processes.toOwnedSlice(allocator);
    }

    fn collectProcess(self: *Collector, allocator: Allocator, pid: u32, current_time: i64) !ProcessInfo {
        // Read /proc/[pid]/stat
        var stat_path_buf: [64]u8 = undefined;
        const stat_path = try std.fmt.bufPrint(&stat_path_buf, "/proc/{d}/stat", .{pid});

        const stat_file = fs.openFileAbsolute(stat_path, .{}) catch return error.ProcessGone;
        defer stat_file.close();

        var stat_buf: [4096]u8 = undefined;
        const stat_len = try stat_file.readAll(&stat_buf);
        const stat_content = stat_buf[0..stat_len];

        // Parse stat file - format: pid (comm) state ...
        const comm_start = std.mem.indexOfScalar(u8, stat_content, '(') orelse return error.InvalidStat;
        const comm_end = std.mem.lastIndexOfScalar(u8, stat_content, ')') orelse return error.InvalidStat;

        const name = try allocator.dupe(u8, stat_content[comm_start + 1 .. comm_end]);
        errdefer allocator.free(name);

        // Parse fields after comm
        const after_comm = stat_content[comm_end + 2 ..]; // Skip ") "
        var it = std.mem.tokenizeScalar(u8, after_comm, ' ');

        const state = (it.next() orelse return error.InvalidStat)[0];

        // Skip fields 4-11 (ppid, pgrp, session, tty_nr, tpgid, flags, minflt, cminflt, majflt, cmajflt)
        var i: usize = 0;
        while (i < 10) : (i += 1) {
            _ = it.next() orelse return error.InvalidStat;
        }

        const utime = try std.fmt.parseInt(u64, it.next() orelse return error.InvalidStat, 10);
        const stime = try std.fmt.parseInt(u64, it.next() orelse return error.InvalidStat, 10);

        // Skip cutime, cstime, priority, nice
        i = 0;
        while (i < 4) : (i += 1) {
            _ = it.next() orelse return error.InvalidStat;
        }

        const threads = try std.fmt.parseInt(u32, it.next() orelse return error.InvalidStat, 10);

        // Skip itrealvalue
        _ = it.next() orelse return error.InvalidStat;

        // Skip starttime
        _ = it.next() orelse return error.InvalidStat;

        // Skip vsize (field 23)
        _ = it.next() orelse return error.InvalidStat;

        // Get RSS (field 24)
        const rss_pages = try std.fmt.parseInt(u64, it.next() orelse return error.InvalidStat, 10);
        const mem_rss = rss_pages * self.page_size;

        // Calculate CPU percentage
        var cpu_percent: f32 = 0.0;
        if (self.prev_processes.get(pid)) |prev_stats| {
            const time_delta = current_time - prev_stats.timestamp;
            if (time_delta > 0) {
                const cpu_delta = (utime + stime) - (prev_stats.utime + prev_stats.stime);
                cpu_percent = 100.0 * @as(f32, @floatFromInt(cpu_delta)) /
                    @as(f32, @floatFromInt(self.clock_ticks * @as(u64, @intCast(time_delta))));
            }
        }

        // Update previous stats
        try self.prev_processes.put(pid, ProcessStats{
            .utime = utime,
            .stime = stime,
            .timestamp = current_time,
        });

        // Read cmdline
        var cmdline_path_buf: [64]u8 = undefined;
        const cmdline_path = try std.fmt.bufPrint(&cmdline_path_buf, "/proc/{d}/cmdline", .{pid});

        const cmdline = blk: {
            const cmdline_file = fs.openFileAbsolute(cmdline_path, .{}) catch {
                break :blk try allocator.dupe(u8, "");
            };
            defer cmdline_file.close();

            var cmdline_buf: [4096]u8 = undefined;
            const cmdline_len = cmdline_file.readAll(&cmdline_buf) catch 0;

            if (cmdline_len == 0) {
                break :blk try allocator.dupe(u8, "");
            }

            // Replace null bytes with spaces
            for (cmdline_buf[0..cmdline_len]) |*byte| {
                if (byte.* == 0) byte.* = ' ';
            }

            // Trim trailing space
            var end = cmdline_len;
            while (end > 0 and cmdline_buf[end - 1] == ' ') : (end -= 1) {}

            break :blk try allocator.dupe(u8, cmdline_buf[0..end]);
        };
        errdefer allocator.free(cmdline);

        // Read UID from status file and resolve username
        const username = blk: {
            var status_path_buf: [64]u8 = undefined;
            const status_path = try std.fmt.bufPrint(&status_path_buf, "/proc/{d}/status", .{pid});

            const status_file = fs.openFileAbsolute(status_path, .{}) catch {
                break :blk try allocator.dupe(u8, "unknown");
            };
            defer status_file.close();

            var status_buf: [4096]u8 = undefined;
            const bytes_read = status_file.readAll(&status_buf) catch {
                break :blk try allocator.dupe(u8, "unknown");
            };
            const content = status_buf[0..bytes_read];

            var uid: u32 = 0;
            var line_it = std.mem.splitScalar(u8, content, '\n');
            while (line_it.next()) |line| {
                if (std.mem.startsWith(u8, line, "Uid:")) {
                    var uid_it = std.mem.tokenizeScalar(u8, line, '\t');
                    _ = uid_it.next(); // Skip "Uid:"
                    const uid_str = uid_it.next() orelse break;
                    uid = std.fmt.parseInt(u32, uid_str, 10) catch break;
                    break;
                }
            }

            break :blk try self.resolveUsername(allocator, uid);
        };
        errdefer allocator.free(username);

        return ProcessInfo{
            .pid = pid,
            .name = name,
            .cmdline = cmdline,
            .state = state,
            .cpu_percent = cpu_percent,
            .mem_rss = mem_rss,
            .threads = threads,
            .username = username,
        };
    }

    fn resolveUsername(self: *Collector, allocator: Allocator, uid: u32) ![]const u8 {
        _ = self;

        // Try to read /etc/passwd
        const passwd_file = fs.openFileAbsolute("/etc/passwd", .{}) catch {
            return std.fmt.allocPrint(allocator, "{d}", .{uid});
        };
        defer passwd_file.close();

        var buf: [32768]u8 = undefined; // /etc/passwd can be large
        const bytes_read = passwd_file.readAll(&buf) catch {
            return std.fmt.allocPrint(allocator, "{d}", .{uid});
        };
        const content = buf[0..bytes_read];

        var line_it = std.mem.splitScalar(u8, content, '\n');
        while (line_it.next()) |line| {
            // Format: username:x:uid:gid:...
            var it = std.mem.tokenizeScalar(u8, line, ':');
            const username = it.next() orelse continue;
            _ = it.next(); // Skip password field
            const uid_str = it.next() orelse continue;

            const line_uid = std.fmt.parseInt(u32, uid_str, 10) catch continue;
            if (line_uid == uid) {
                return allocator.dupe(u8, username);
            }
        }

        // Fallback to UID as string
        return std.fmt.allocPrint(allocator, "{d}", .{uid});
    }

    // ========================================================================
    // Disk Collection
    // ========================================================================

    pub fn collectDisks(allocator: Allocator) ![]DiskInfo {
        var disks = try std.ArrayList(DiskInfo).initCapacity(allocator, 8);
        errdefer {
            for (disks.items) |disk| {
                allocator.free(disk.mount_point);
                allocator.free(disk.filesystem);
            }
            disks.deinit(allocator);
        }

        // Track seen devices to skip duplicate mounts (e.g. /bin, /usr, /lib on same device)
        var seen_devices = std.StringHashMap(usize).init(allocator);
        defer seen_devices.deinit();

        const mounts_file = try fs.openFileAbsolute("/proc/mounts", .{});
        defer mounts_file.close();

        var buf: [32768]u8 = undefined; // /proc/mounts can have many entries
        const bytes_read = try mounts_file.readAll(&buf);
        const content = buf[0..bytes_read];

        var line_it = std.mem.splitScalar(u8, content, '\n');
        while (line_it.next()) |line| {
            if (line.len == 0) continue;
            var it = std.mem.tokenizeScalar(u8, line, ' ');

            const device = it.next() orelse continue;
            const mount_point = it.next() orelse continue;
            const fstype = it.next() orelse continue;

            // Skip virtual/pseudo filesystems
            if (std.mem.startsWith(u8, fstype, "tmpfs") or
                std.mem.startsWith(u8, fstype, "devtmpfs") or
                std.mem.startsWith(u8, fstype, "sysfs") or
                std.mem.startsWith(u8, fstype, "proc") or
                std.mem.startsWith(u8, fstype, "devpts") or
                std.mem.startsWith(u8, fstype, "cgroup") or
                std.mem.startsWith(u8, fstype, "securityfs") or
                std.mem.startsWith(u8, fstype, "pstore") or
                std.mem.startsWith(u8, fstype, "autofs") or
                std.mem.startsWith(u8, fstype, "mqueue") or
                std.mem.startsWith(u8, fstype, "hugetlbfs") or
                std.mem.startsWith(u8, fstype, "debugfs") or
                std.mem.startsWith(u8, fstype, "tracefs") or
                std.mem.startsWith(u8, fstype, "fusectl") or
                std.mem.startsWith(u8, fstype, "configfs") or
                std.mem.startsWith(u8, fstype, "bpf") or
                std.mem.startsWith(u8, fstype, "binfmt") or
                std.mem.startsWith(u8, fstype, "selinux"))
            {
                continue;
            }

            // Deduplicate by device, keeping the shortest mount path (e.g. "/" over "/usr")
            const gop = try seen_devices.getOrPut(device);
            if (gop.found_existing) {
                if (mount_point.len < gop.value_ptr.*) {
                    for (disks.items, 0..) |disk, idx| {
                        if (std.mem.eql(u8, disk.filesystem, device)) {
                            allocator.free(disks.items[idx].mount_point);
                            disks.items[idx].mount_point = try allocator.dupe(u8, mount_point);
                            gop.value_ptr.* = mount_point.len;
                            break;
                        }
                    }
                }
                continue;
            }
            gop.value_ptr.* = mount_point.len;

            // Get disk stats using statvfs via direct syscall
            const Statvfs = extern struct {
                f_bsize: u64,
                f_frsize: u64,
                f_blocks: u64,
                f_bfree: u64,
                f_bavail: u64,
                f_files: u64,
                f_ffree: u64,
                f_favail: u64,
                f_fsid: u64,
                f_flag: u64,
                f_namemax: u64,
            };

            var stat: Statvfs = undefined;
            const mount_point_z = try allocator.dupeZ(u8, mount_point);
            defer allocator.free(mount_point_z);

            const result = os.linux.syscall2(.statfs, @intFromPtr(mount_point_z.ptr), @intFromPtr(&stat));
            if (result != 0) continue;

            const block_size = stat.f_frsize;
            const total_bytes = stat.f_blocks * block_size;
            const free_bytes = stat.f_bfree * block_size;
            const used_bytes = if (total_bytes > free_bytes) total_bytes - free_bytes else 0;

            const percent = if (total_bytes > 0)
                100.0 * @as(f32, @floatFromInt(used_bytes)) / @as(f32, @floatFromInt(total_bytes))
            else
                0.0;

            try disks.append(allocator, DiskInfo{
                .mount_point = try allocator.dupe(u8, mount_point),
                .filesystem = try allocator.dupe(u8, device),
                .total_bytes = total_bytes,
                .used_bytes = used_bytes,
                .percent = percent,
            });
        }

        return disks.toOwnedSlice(allocator);
    }
};

// ============================================================================
// Tests
// ============================================================================

test "init and deinit" {
    const allocator = std.testing.allocator;
    var collector = try Collector.init(allocator);
    defer collector.deinit();
}

test "collect metrics" {
    const allocator = std.testing.allocator;
    var collector = try Collector.init(allocator);
    defer collector.deinit();

    // First collection (no previous data)
    const metrics1 = try collector.collectMetrics();
    try std.testing.expect(metrics1.mem_total > 0);
    try std.testing.expect(metrics1.cpu_percent == 0.0); // First sample has no delta

    // Second collection (should have CPU data)
    std.Thread.sleep(100 * std.time.ns_per_ms); // Sleep 100ms
    const metrics2 = try collector.collectMetrics();
    try std.testing.expect(metrics2.mem_total > 0);
    try std.testing.expect(metrics2.mem_percent >= 0.0 and metrics2.mem_percent <= 100.0);
}

test "collect processes" {
    const allocator = std.testing.allocator;
    var collector = try Collector.init(allocator);
    defer collector.deinit();

    const processes = try collector.collectProcesses(allocator);
    defer {
        for (processes) |proc| {
            allocator.free(proc.name);
            allocator.free(proc.cmdline);
            allocator.free(proc.username);
        }
        allocator.free(processes);
    }

    try std.testing.expect(processes.len > 0); // Should have at least some processes

    // Verify our own process exists
    const my_pid = std.os.linux.getpid();
    var found = false;
    for (processes) |proc| {
        if (proc.pid == my_pid) {
            found = true;
            try std.testing.expect(proc.mem_rss > 0);
            try std.testing.expect(proc.threads > 0);
            break;
        }
    }
    try std.testing.expect(found);
}

test "collect disks" {
    const allocator = std.testing.allocator;

    const disks = try Collector.collectDisks(allocator);
    defer {
        for (disks) |disk| {
            allocator.free(disk.mount_point);
            allocator.free(disk.filesystem);
        }
        allocator.free(disks);
    }

    try std.testing.expect(disks.len > 0); // Should have at least root filesystem

    // Verify root filesystem exists
    var found_root = false;
    for (disks) |disk| {
        if (std.mem.eql(u8, disk.mount_point, "/")) {
            found_root = true;
            try std.testing.expect(disk.total_bytes > 0);
            try std.testing.expect(disk.percent >= 0.0 and disk.percent <= 100.0);
            break;
        }
    }
    try std.testing.expect(found_root);
}
