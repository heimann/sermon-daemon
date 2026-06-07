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
    // Cumulative bytes the process has caused to be fetched from / sent to the
    // storage layer (from /proc/[pid]/io). 0 when the file is unreadable.
    io_read_bytes: u64,
    io_write_bytes: u64,
    // Raw cgroup path (last line of /proc/[pid]/cgroup) and a friendly unit
    // name derived from it (systemd unit, docker/k8s container). Both are
    // owned strings; cgroup is "" when /proc/[pid]/cgroup is unreadable and
    // unit is "" when no friendly name could be derived.
    cgroup: []const u8,
    unit: []const u8,
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
    starttime: u64, // /proc/[pid]/stat field 22; identifies the process across PID reuse
    timestamp: i64,
};

pub const Collector = struct {
    allocator: Allocator,
    prev_cpu: ?CpuStats,
    prev_processes: std.AutoHashMap(u32, ProcessStats),
    clock_ticks: u64,
    page_size: u64,
    // Cap on processes returned per cycle. collectProcesses keeps the union of
    // the top-N by CPU and top-N by memory; everything else is dropped before
    // being returned. keep_all_processes disables trimming (stores every
    // process, the original behavior).
    max_processes: u32,

    /// Sentinel for `max_processes`: keep every process, no trimming.
    pub const keep_all_processes: u32 = 0;

    pub fn init(allocator: Allocator) !Collector {
        const clock_ticks = readClockTicks() catch 100;

        return Collector{
            .allocator = allocator,
            .prev_cpu = null,
            .prev_processes = std.AutoHashMap(u32, ProcessStats).init(allocator),
            .clock_ticks = clock_ticks,
            .page_size = @intCast(std.c.sysconf(@intFromEnum(std.c._SC.PAGESIZE))),
            .max_processes = keep_all_processes,
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
                allocator.free(proc.cgroup);
                allocator.free(proc.unit);
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

        // Drop CPU-delta baselines for PIDs that no longer exist before any
        // returned/stored-process trimming. Pruning must see the full live set.
        try self.pruneDeadProcesses(allocator, processes.items);

        // Trim to the most interesting processes for this cycle. This must run
        // AFTER the full list is built and pruned: collectProcess updates
        // prev_processes (the CPU-delta baseline) for every PID, so trimming
        // earlier would starve future cycles of deltas and break pruning.
        if (self.max_processes != keep_all_processes and
            processes.items.len > self.max_processes)
        {
            try self.trimToTopN(allocator, &processes, self.max_processes);
        }

        // Enrich only the kept set with per-process I/O + cgroup/unit. Bounded
        // to ~max_processes reads instead of one per process on the box - this
        // is what keeps the new signals inside the CPU budget. A pid that exited
        // since collection just gets 0 / "".
        for (processes.items) |*proc| {
            try enrichProcess(allocator, proc);
        }

        return processes.toOwnedSlice(allocator);
    }

    fn pruneDeadProcesses(self: *Collector, allocator: Allocator, alive: []const ProcessInfo) !void {
        if (self.prev_processes.count() <= alive.len) return;

        var seen = std.AutoHashMap(u32, void).init(allocator);
        defer seen.deinit();
        try seen.ensureTotalCapacity(@intCast(alive.len));
        for (alive) |proc| seen.putAssumeCapacity(proc.pid, {});

        var stale = std.ArrayList(u32){};
        defer stale.deinit(allocator);
        var it = self.prev_processes.keyIterator();
        while (it.next()) |key_ptr| {
            if (!seen.contains(key_ptr.*)) try stale.append(allocator, key_ptr.*);
        }
        for (stale.items) |pid| _ = self.prev_processes.remove(pid);
    }

    /// Keep the union of the top `n` processes by CPU and the top `n` by
    /// resident memory, deduplicated by PID, and free the owned strings of
    /// everything dropped. On return `processes` holds only the kept entries,
    /// each still fully owned by the caller.
    fn trimToTopN(self: *Collector, allocator: Allocator, processes: *std.ArrayList(ProcessInfo), n: u32) !void {
        _ = self;
        const items = processes.items;

        // First-seen processes have NaN cpu_percent (no prior delta). Treat NaN
        // as 0 so ranking is total and they sort to the bottom on CPU.
        const cpuKey = struct {
            fn key(p: ProcessInfo) f32 {
                return if (std.math.isNan(p.cpu_percent)) 0.0 else p.cpu_percent;
            }
        }.key;

        // Mark the PIDs we keep. An index set avoids mutating the list while we
        // still need the original ordering to pick winners.
        var keep = try std.DynamicBitSet.initEmpty(allocator, items.len);
        defer keep.deinit();

        // Order is a scratch index array we re-sort twice (by CPU, then by mem).
        const order = try allocator.alloc(usize, items.len);
        defer allocator.free(order);
        for (order, 0..) |*o, i| o.* = i;

        const SortCtx = struct {
            items: []const ProcessInfo,
            by_cpu: bool,
            fn lessThan(ctx: @This(), a: usize, b: usize) bool {
                if (ctx.by_cpu) {
                    return cpuKey(ctx.items[a]) > cpuKey(ctx.items[b]);
                }
                return ctx.items[a].mem_rss > ctx.items[b].mem_rss;
            }
        };

        std.mem.sort(usize, order, SortCtx{ .items = items, .by_cpu = true }, SortCtx.lessThan);
        for (order[0..n]) |idx| keep.set(idx);

        std.mem.sort(usize, order, SortCtx{ .items = items, .by_cpu = false }, SortCtx.lessThan);
        for (order[0..n]) |idx| keep.set(idx);

        // Compact in place: free dropped entries' owned strings, shift kept
        // ones down. The trailing slots past `kept` are stale duplicates we
        // must not free again, so shrink the list to drop them.
        var kept: usize = 0;
        for (items, 0..) |proc, i| {
            if (keep.isSet(i)) {
                items[kept] = proc;
                kept += 1;
            } else {
                allocator.free(proc.name);
                allocator.free(proc.cmdline);
                allocator.free(proc.username);
                allocator.free(proc.cgroup);
                allocator.free(proc.unit);
            }
        }
        processes.shrinkRetainingCapacity(kept);
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

        // starttime (field 22): process start time in clock ticks since boot.
        // A recycled PID gets a different starttime, so we key CPU-delta
        // continuity on it rather than trusting the PID alone.
        const starttime = try std.fmt.parseInt(u64, it.next() orelse return error.InvalidStat, 10);

        // Skip vsize (field 23)
        _ = it.next() orelse return error.InvalidStat;

        // Get RSS (field 24)
        const rss_pages = try std.fmt.parseInt(u64, it.next() orelse return error.InvalidStat, 10);
        const mem_rss = rss_pages * self.page_size;

        // Calculate CPU percentage
        var cpu_percent: f32 = 0.0;
        if (self.prev_processes.get(pid)) |prev_stats| {
            const time_delta = current_time - prev_stats.timestamp;
            // Only compute a delta for the SAME process: a recycled PID has a
            // different starttime, so its counters are unrelated to the stored
            // baseline (subtracting them would be meaningless and could underflow
            // and panic). On reuse we skip this cycle and re-baseline below.
            if (time_delta > 0 and prev_stats.starttime == starttime) {
                const cur_jiffies = utime + stime;
                const prev_jiffies = prev_stats.utime + prev_stats.stime;
                // Counters are monotonic for a live process; guard the
                // subtraction defensively so a kernel counter reset can't wrap.
                const cpu_delta = if (cur_jiffies >= prev_jiffies) cur_jiffies - prev_jiffies else 0;
                cpu_percent = 100.0 * @as(f32, @floatFromInt(cpu_delta)) /
                    @as(f32, @floatFromInt(self.clock_ticks * @as(u64, @intCast(time_delta))));
            }
        }

        // Update previous stats
        try self.prev_processes.put(pid, ProcessStats{
            .utime = utime,
            .stime = stime,
            .starttime = starttime,
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

        // Per-process I/O and cgroup/unit are NOT read here. Reading
        // /proc/<pid>/{io,cgroup} for every process on the box each cycle blew
        // the CPU budget (the box can have hundreds of processes; we only keep
        // ~20). They are filled in by enrichProcess() AFTER trimToTopN, so the
        // extra syscalls run only for the handful of processes we actually
        // return. Start them empty/zero.
        const cgroup = try allocator.dupe(u8, "");
        errdefer allocator.free(cgroup);

        const unit = try allocator.dupe(u8, "");
        errdefer allocator.free(unit);

        return ProcessInfo{
            .pid = pid,
            .name = name,
            .cmdline = cmdline,
            .state = state,
            .cpu_percent = cpu_percent,
            .mem_rss = mem_rss,
            .threads = threads,
            .username = username,
            .io_read_bytes = 0,
            .io_write_bytes = 0,
            .cgroup = cgroup,
            .unit = unit,
        };
    }

    /// Fill in the per-process I/O + cgroup/unit fields for one already-collected
    /// process. Called only on the trimmed top-N set so the /proc/<pid>/{io,
    /// cgroup} reads stay bounded per cycle. Replaces the empty cgroup/unit
    /// placeholders set by collectProcess. The pid may have exited since
    /// collection, so every read degrades to 0 / "" rather than erroring.
    fn enrichProcess(allocator: Allocator, proc: *ProcessInfo) !void {
        const io = readProcessIo(proc.pid);
        proc.io_read_bytes = io.read_bytes;
        proc.io_write_bytes = io.write_bytes;

        var cgroup_path_buf: [64]u8 = undefined;
        const cgroup_path = std.fmt.bufPrint(&cgroup_path_buf, "/proc/{d}/cgroup", .{proc.pid}) catch return;

        const cgroup = blk: {
            const cgroup_file = fs.openFileAbsolute(cgroup_path, .{}) catch break :blk try allocator.dupe(u8, "");
            defer cgroup_file.close();

            var cgroup_buf: [4096]u8 = undefined;
            const cgroup_len = cgroup_file.readAll(&cgroup_buf) catch 0;
            if (cgroup_len == 0) break :blk try allocator.dupe(u8, "");

            // cgroup v2 emits a single "0::<path>" line; v1 emits several. The
            // unified (v2) line is the most useful, so prefer the last non-empty
            // line and keep only the part after the final ':'.
            var line_it = std.mem.splitScalar(u8, cgroup_buf[0..cgroup_len], '\n');
            var last_line: []const u8 = "";
            while (line_it.next()) |line| {
                if (line.len > 0) last_line = line;
            }
            const path = if (std.mem.lastIndexOfScalar(u8, last_line, ':')) |idx|
                last_line[idx + 1 ..]
            else
                last_line;

            break :blk try allocator.dupe(u8, path);
        };

        const unit = try deriveUnit(allocator, cgroup);

        // Replace the empty placeholders set by collectProcess.
        allocator.free(proc.cgroup);
        allocator.free(proc.unit);
        proc.cgroup = cgroup;
        proc.unit = unit;
    }

    const ProcessIo = struct { read_bytes: u64, write_bytes: u64 };

    /// Parse read_bytes/write_bytes out of /proc/[pid]/io. Never errors: any
    /// open/read/parse failure (missing file, kernel thread, exited pid) yields
    /// 0/0 so the collection cycle can't be broken by one unreadable process.
    fn readProcessIo(pid: u32) ProcessIo {
        var io_path_buf: [64]u8 = undefined;
        const io_path = std.fmt.bufPrint(&io_path_buf, "/proc/{d}/io", .{pid}) catch return .{ .read_bytes = 0, .write_bytes = 0 };

        const io_file = fs.openFileAbsolute(io_path, .{}) catch return .{ .read_bytes = 0, .write_bytes = 0 };
        defer io_file.close();

        var io_buf: [4096]u8 = undefined;
        const io_len = io_file.readAll(&io_buf) catch return .{ .read_bytes = 0, .write_bytes = 0 };

        return parseProcessIo(io_buf[0..io_len]);
    }

    /// Pull the "read_bytes:" and "write_bytes:" lines out of /proc/[pid]/io
    /// content. Unknown/garbled lines are ignored; missing keys stay 0.
    fn parseProcessIo(content: []const u8) ProcessIo {
        var read_bytes: u64 = 0;
        var write_bytes: u64 = 0;

        var line_it = std.mem.splitScalar(u8, content, '\n');
        while (line_it.next()) |line| {
            var it = std.mem.tokenizeAny(u8, line, ": ");
            const key = it.next() orelse continue;
            const value_str = it.next() orelse continue;
            const value = std.fmt.parseInt(u64, value_str, 10) catch continue;
            if (std.mem.eql(u8, key, "read_bytes")) {
                read_bytes = value;
            } else if (std.mem.eql(u8, key, "write_bytes")) {
                write_bytes = value;
            }
        }

        return .{ .read_bytes = read_bytes, .write_bytes = write_bytes };
    }

    /// Derive a friendly unit name from a cgroup path. Recognizes systemd
    /// units (".service"/".scope"/".slice"), docker containers, and k8s pods.
    /// Returns an owned string; "" when nothing recognizable was found. The
    /// caller owns the result and must free it.
    fn deriveUnit(allocator: Allocator, cgroup: []const u8) ![]const u8 {
        if (cgroup.len == 0) return allocator.dupe(u8, "");

        // The last path segment usually carries the identity (e.g.
        // ".../dmeh.service", ".../docker-<id>.scope").
        var seg_it = std.mem.splitScalar(u8, cgroup, '/');
        var last_seg: []const u8 = "";
        while (seg_it.next()) |seg| {
            if (seg.len > 0) last_seg = seg;
        }

        // Docker: "docker-<64hex>.scope" or a bare "docker/<id>" path.
        if (std.mem.indexOf(u8, cgroup, "docker") != null) {
            if (extractContainerId(last_seg, "docker-")) |id| {
                return std.fmt.allocPrint(allocator, "docker:{s}", .{id});
            }
        }

        // Kubernetes: pods live under a "kubepods" slice. Scan for the segment
        // that names the pod (starts with "pod"); the final segment is usually
        // the container id which is less useful on its own.
        if (std.mem.indexOf(u8, cgroup, "kubepods") != null) {
            var k8s_it = std.mem.splitScalar(u8, cgroup, '/');
            while (k8s_it.next()) |seg| {
                if (std.mem.startsWith(u8, seg, "pod") and seg.len > 3) {
                    return std.fmt.allocPrint(allocator, "k8s:{s}", .{seg});
                }
            }
            return allocator.dupe(u8, "k8s");
        }

        // systemd unit: the segment ends in a known suffix. .slice is the
        // least specific, so it's the fallback.
        if (std.mem.endsWith(u8, last_seg, ".service") or
            std.mem.endsWith(u8, last_seg, ".scope") or
            std.mem.endsWith(u8, last_seg, ".slice") or
            std.mem.endsWith(u8, last_seg, ".mount") or
            std.mem.endsWith(u8, last_seg, ".socket"))
        {
            return allocator.dupe(u8, last_seg);
        }

        return allocator.dupe(u8, "");
    }

    /// Pull the container id out of a segment like "docker-<id>.scope". Returns
    /// the id slice (a view into `seg`) or null if the shape doesn't match.
    fn extractContainerId(seg: []const u8, prefix: []const u8) ?[]const u8 {
        if (!std.mem.startsWith(u8, seg, prefix)) return null;
        var id = seg[prefix.len..];
        if (std.mem.lastIndexOfScalar(u8, id, '.')) |dot| id = id[0..dot];
        if (id.len == 0) return null;
        return id;
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
            allocator.free(proc.cgroup);
            allocator.free(proc.unit);
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

test "prunes dead pids from prev_processes" {
    const allocator = std.testing.allocator;
    var collector = try Collector.init(allocator);
    defer collector.deinit();

    // Seed baselines for PIDs that cannot exist (above pid_max).
    const fake_pids = [_]u32{ 4_000_000_001, 4_000_000_002, 4_000_000_003 };
    for (fake_pids) |pid| {
        try collector.prev_processes.put(pid, .{ .utime = 1, .stime = 1, .starttime = 0, .timestamp = 0 });
    }

    const processes = try collector.collectProcesses(allocator);
    defer {
        for (processes) |proc| {
            allocator.free(proc.name);
            allocator.free(proc.cmdline);
            allocator.free(proc.username);
            allocator.free(proc.cgroup);
            allocator.free(proc.unit);
        }
        allocator.free(processes);
    }

    // Dead PIDs must be gone; the map should not exceed the live process count.
    for (fake_pids) |pid| try std.testing.expect(!collector.prev_processes.contains(pid));
    try std.testing.expect(collector.prev_processes.count() <= processes.len);
}

test "collect processes top-n trimming end to end" {
    const allocator = std.testing.allocator;
    var collector = try Collector.init(allocator);
    defer collector.deinit();

    const n: u32 = 5;
    collector.max_processes = n;

    const processes = try collector.collectProcesses(allocator);
    defer {
        for (processes) |proc| {
            allocator.free(proc.name);
            allocator.free(proc.cmdline);
            allocator.free(proc.username);
            allocator.free(proc.cgroup);
            allocator.free(proc.unit);
        }
        allocator.free(processes);
    }

    // Real hosts run far more than 2*n processes, so trimming must engage and
    // the union of top-n-by-cpu and top-n-by-mem caps the result at 2*n.
    try std.testing.expect(processes.len > 0);
    try std.testing.expect(processes.len <= 2 * n);
}

// Drives trimToTopN with a synthetic list so the selection invariant is
// deterministic: the global highest-cpu and highest-mem entries must survive,
// dropped entries' strings are freed, and the result is the deduplicated union.
test "trimToTopN keeps top cpu and top mem" {
    const allocator = std.testing.allocator;
    var collector = try Collector.init(allocator);
    defer collector.deinit();

    const Spec = struct { pid: u32, cpu: f32, mem: u64 };
    const specs = [_]Spec{
        .{ .pid = 1, .cpu = 90.0, .mem = 100 }, // top cpu
        .{ .pid = 2, .cpu = 1.0, .mem = 9000 }, // top mem
        .{ .pid = 3, .cpu = std.math.nan(f32), .mem = 50 }, // NaN cpu -> ranks as 0
        .{ .pid = 4, .cpu = 5.0, .mem = 200 },
        .{ .pid = 5, .cpu = 2.0, .mem = 300 },
        .{ .pid = 6, .cpu = 0.5, .mem = 10 }, // should be dropped
        .{ .pid = 7, .cpu = 0.1, .mem = 5 }, // should be dropped
    };

    var processes = try std.ArrayList(ProcessInfo).initCapacity(allocator, specs.len);
    defer {
        for (processes.items) |proc| {
            allocator.free(proc.name);
            allocator.free(proc.cmdline);
            allocator.free(proc.username);
            allocator.free(proc.cgroup);
            allocator.free(proc.unit);
        }
        processes.deinit(allocator);
    }
    for (specs) |s| {
        try processes.append(allocator, .{
            .pid = s.pid,
            .name = try allocator.dupe(u8, "p"),
            .cmdline = try allocator.dupe(u8, "c"),
            .username = try allocator.dupe(u8, "u"),
            .state = 'R',
            .cpu_percent = s.cpu,
            .mem_rss = s.mem,
            .threads = 1,
            .io_read_bytes = 0,
            .io_write_bytes = 0,
            .cgroup = try allocator.dupe(u8, ""),
            .unit = try allocator.dupe(u8, ""),
        });
    }

    const n: u32 = 2;
    try collector.trimToTopN(allocator, &processes, n);

    // Top-2 by cpu = {1, 4}; top-2 by mem = {2, 5}; union = 4 entries.
    try std.testing.expectEqual(@as(usize, 4), processes.items.len);

    var has_top_cpu = false;
    var has_top_mem = false;
    for (processes.items) |proc| {
        if (proc.pid == 1) has_top_cpu = true;
        if (proc.pid == 2) has_top_mem = true;
        // The two lowest-ranked entries must have been dropped.
        try std.testing.expect(proc.pid != 6 and proc.pid != 7);
    }
    try std.testing.expect(has_top_cpu);
    try std.testing.expect(has_top_mem);
}

test "parseProcessIo extracts read_bytes and write_bytes" {
    const content =
        \\rchar: 123456
        \\wchar: 7890
        \\syscr: 10
        \\syscw: 20
        \\read_bytes: 4096
        \\write_bytes: 8192
        \\cancelled_write_bytes: 0
    ;
    const io = Collector.parseProcessIo(content);
    try std.testing.expectEqual(@as(u64, 4096), io.read_bytes);
    try std.testing.expectEqual(@as(u64, 8192), io.write_bytes);
}

test "parseProcessIo tolerates missing keys and garbage" {
    // Only write_bytes present; read_bytes stays 0, junk lines ignored.
    const content =
        \\garbage line with no colon
        \\write_bytes: 555
        \\read_bytes: notanumber
    ;
    const io = Collector.parseProcessIo(content);
    try std.testing.expectEqual(@as(u64, 0), io.read_bytes);
    try std.testing.expectEqual(@as(u64, 555), io.write_bytes);
}

test "readProcessIo returns 0/0 for a missing pid" {
    // A pid far above pid_max cannot exist; the read must degrade to 0/0.
    const io = Collector.readProcessIo(4_000_000_001);
    try std.testing.expectEqual(@as(u64, 0), io.read_bytes);
    try std.testing.expectEqual(@as(u64, 0), io.write_bytes);
}

test "deriveUnit recognizes systemd, docker, k8s, and unknown cgroups" {
    const allocator = std.testing.allocator;

    const Case = struct { cgroup: []const u8, want: []const u8 };
    const cases = [_]Case{
        .{ .cgroup = "/system.slice/dmeh.service", .want = "dmeh.service" },
        .{ .cgroup = "/user.slice/user-1000.slice/session-2.scope", .want = "session-2.scope" },
        .{ .cgroup = "/system.slice", .want = "system.slice" },
        .{ .cgroup = "/system.slice/docker-abc123def456.scope", .want = "docker:abc123def456" },
        .{ .cgroup = "/kubepods/burstable/pod1234-5678/abcd", .want = "k8s:pod1234-5678" },
        .{ .cgroup = "/some/unknown/path", .want = "" },
        .{ .cgroup = "", .want = "" },
    };

    for (cases) |cse| {
        const unit = try Collector.deriveUnit(allocator, cse.cgroup);
        defer allocator.free(unit);
        try std.testing.expectEqualStrings(cse.want, unit);
    }
}

test "collect processes populates io and cgroup fields" {
    const allocator = std.testing.allocator;
    var collector = try Collector.init(allocator);
    defer collector.deinit();

    const processes = try collector.collectProcesses(allocator);
    defer {
        for (processes) |proc| {
            allocator.free(proc.name);
            allocator.free(proc.cmdline);
            allocator.free(proc.username);
            allocator.free(proc.cgroup);
            allocator.free(proc.unit);
        }
        allocator.free(processes);
    }

    // Our own process should have a cgroup path under cgroup v2/v1.
    const my_pid = std.os.linux.getpid();
    for (processes) |proc| {
        if (proc.pid == @as(u32, @intCast(my_pid))) {
            // cgroup/unit are owned strings (possibly empty); io fields exist.
            _ = proc.io_read_bytes;
            _ = proc.io_write_bytes;
            break;
        }
    }
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
