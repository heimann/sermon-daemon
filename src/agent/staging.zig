//! Durable append-log staging tier for the parquet hot path (plan 25).
//!
//! The daemon's always-on write path is a tiny, allocation-light append log -
//! NOT a resident DuckDB. One segment file per table lives at
//! `<root>/_staging/<table>.log`. Each collect cycle appends a single framed
//! record per table and issues one `fdatasync` per touched segment, so a crash
//! loses nothing that was committed.
//!
//! Record framing (little-endian):
//!
//!     [u32 payload_len][u32 crc32(payload)][payload bytes]
//!
//! The payload is a fixed, schema-versioned binary encoding of one cycle's rows
//! built from the daemon's existing typed structs (collector.SystemMetrics /
//! ProcessInfo / DiskInfo, logs.LogEntry, proxmox.ContainerEntry). The very
//! first byte of a fresh segment is a 1-byte `schema_version`; it is written
//! once at file creation and is NOT part of any record frame.
//!
//! On replay (used both by the roll and the query snapshot) a `StagingReader`
//! validates each record's length + CRC and STOPS at the first short read or
//! bad CRC - a torn tail from a power loss mid-append is silently dropped while
//! every fully-committed record before it is still returned.

const std = @import("std");
const Allocator = std.mem.Allocator;
const fs = std.fs;

const collector = @import("collector");
const logs = @import("logs");
const proxmox = @import("proxmox");

pub const SystemMetrics = collector.SystemMetrics;
pub const ProcessInfo = collector.ProcessInfo;
pub const DiskInfo = collector.DiskInfo;
pub const LogEntry = logs.LogEntry;
pub const ContainerEntry = proxmox.ContainerEntry;

pub const StagingError = error{
    CorruptSegment,
    UnsupportedSchemaVersion,
};

/// Bumped whenever the on-disk payload encoding changes incompatibly. Written
/// as the first byte of a fresh segment and verified on open/replay.
pub const schema_version: u8 = 1;

/// The five tables the hot tier carries. The names match today's DuckDB table
/// names exactly so the query path can present compatibility views.
pub const Table = enum {
    metrics,
    processes,
    disks,
    containers,
    logs,

    pub fn name(self: Table) []const u8 {
        return @tagName(self);
    }

    pub const all = [_]Table{ .metrics, .processes, .disks, .containers, .logs };
};

// File mode matches the DuckDB on-disk DB (0600): an unprivileged user on the
// host must not be able to read collected metrics/logs.
const segment_mode: fs.File.Mode = 0o600;

const staging_subdir = "_staging";

// ============================================================================
// Payload encoding
// ============================================================================

/// Append a length-prefixed string to `buf`.
fn putStr(buf: *std.ArrayList(u8), allocator: Allocator, s: []const u8) !void {
    try putU32(buf, allocator, @intCast(s.len));
    try buf.appendSlice(allocator, s);
}

fn putU32(buf: *std.ArrayList(u8), allocator: Allocator, v: u32) !void {
    var tmp: [4]u8 = undefined;
    std.mem.writeInt(u32, &tmp, v, .little);
    try buf.appendSlice(allocator, &tmp);
}

fn putU64(buf: *std.ArrayList(u8), allocator: Allocator, v: u64) !void {
    var tmp: [8]u8 = undefined;
    std.mem.writeInt(u64, &tmp, v, .little);
    try buf.appendSlice(allocator, &tmp);
}

fn putI64(buf: *std.ArrayList(u8), allocator: Allocator, v: i64) !void {
    try putU64(buf, allocator, @bitCast(v));
}

fn putF32(buf: *std.ArrayList(u8), allocator: Allocator, v: f32) !void {
    try putU32(buf, allocator, @bitCast(v));
}

fn putF64(buf: *std.ArrayList(u8), allocator: Allocator, v: f64) !void {
    try putU64(buf, allocator, @bitCast(v));
}

/// Encode an optional string: a 1-byte present flag then the string when set.
fn putOptStr(buf: *std.ArrayList(u8), allocator: Allocator, s: ?[]const u8) !void {
    if (s) |val| {
        try buf.append(allocator, 1);
        try putStr(buf, allocator, val);
    } else {
        try buf.append(allocator, 0);
    }
}

fn putOptU32(buf: *std.ArrayList(u8), allocator: Allocator, v: ?u32) !void {
    if (v) |val| {
        try buf.append(allocator, 1);
        try putU32(buf, allocator, val);
    } else {
        try buf.append(allocator, 0);
    }
}

/// Cursor over a payload buffer that decodes the same encoding `put*` writes.
/// Every read is bounds-checked; a truncated payload yields CorruptSegment so
/// the caller can treat it as a torn record.
const Cursor = struct {
    buf: []const u8,
    pos: usize = 0,

    fn need(self: *Cursor, n: usize) ![]const u8 {
        if (self.pos + n > self.buf.len) return StagingError.CorruptSegment;
        const out = self.buf[self.pos .. self.pos + n];
        self.pos += n;
        return out;
    }

    fn u32_(self: *Cursor) !u32 {
        return std.mem.readInt(u32, (try self.need(4))[0..4], .little);
    }

    fn u64_(self: *Cursor) !u64 {
        return std.mem.readInt(u64, (try self.need(8))[0..8], .little);
    }

    fn i64_(self: *Cursor) !i64 {
        return @bitCast(try self.u64_());
    }

    fn f32_(self: *Cursor) !f32 {
        return @bitCast(try self.u32_());
    }

    fn f64_(self: *Cursor) !f64 {
        return @bitCast(try self.u64_());
    }

    /// Decode a length-prefixed string into an owned copy.
    fn str(self: *Cursor, allocator: Allocator) ![]const u8 {
        const len = try self.u32_();
        const bytes = try self.need(len);
        return allocator.dupe(u8, bytes);
    }

    fn optStr(self: *Cursor, allocator: Allocator) !?[]const u8 {
        const present = (try self.need(1))[0];
        if (present == 0) return null;
        return try self.str(allocator);
    }

    fn optU32(self: *Cursor) !?u32 {
        const present = (try self.need(1))[0];
        if (present == 0) return null;
        return try self.u32_();
    }
};

// ============================================================================
// Decoded cycle rows (returned by StagingReader)
// ============================================================================

/// One decoded cycle for the metrics table. The metrics struct itself carries
/// no timestamp, so the cycle timestamp is kept alongside.
pub const MetricsRow = struct {
    timestamp: i64,
    metrics: SystemMetrics,
};

/// A decoded cycle's rows for one table. Owns all heap allocations; free with
/// deinit. Only the variant matching the segment's table is populated.
pub const DecodedCycle = union(Table) {
    metrics: []MetricsRow,
    processes: TimedSlice(ProcessInfo),
    disks: TimedSlice(DiskInfo),
    containers: TimedSlice(ContainerEntry),
    logs: []LogEntry,
};

/// A per-cycle slice of rows sharing a single cycle timestamp (mirrors how
/// storage.zig's insert* take one timestamp for the whole batch).
pub fn TimedSlice(comptime T: type) type {
    return struct {
        timestamp: i64,
        rows: []T,
    };
}

// ============================================================================
// Staging (writer)
// ============================================================================

pub const Staging = struct {
    allocator: Allocator,
    root_dir: []u8,
    /// One open append fd per table, indexed by @intFromEnum(Table).
    files: [Table.all.len]fs.File,
    /// Reusable scratch buffer for building a record payload. Kept across
    /// cycles so the hot path allocates nothing in steady state.
    scratch: std.ArrayList(u8),

    /// Open (creating if needed) every table's segment under
    /// `<root_dir>/_staging/`. A fresh segment gets its 1-byte schema_version
    /// written immediately; an existing segment is verified to carry a
    /// compatible version. Each fd is opened append-only, mode 0600.
    pub fn open(allocator: Allocator, root_dir: []const u8) !Staging {
        const owned_root = try allocator.dupe(u8, root_dir);
        errdefer allocator.free(owned_root);

        const staging_dir = try std.fs.path.join(allocator, &.{ root_dir, staging_subdir });
        defer allocator.free(staging_dir);
        try makeDirAbsoluteIfAbsent(staging_dir);

        var files: [Table.all.len]fs.File = undefined;
        var opened: usize = 0;
        errdefer for (files[0..opened]) |*f| f.close();

        for (Table.all) |table| {
            const path = try segmentPath(allocator, root_dir, table);
            defer allocator.free(path);
            files[@intFromEnum(table)] = try openSegment(path);
            opened += 1;
        }

        return Staging{
            .allocator = allocator,
            .root_dir = owned_root,
            .files = files,
            .scratch = .{},
        };
    }

    pub fn deinit(self: *Staging) void {
        for (&self.files) |*f| f.close();
        self.scratch.deinit(self.allocator);
        self.allocator.free(self.root_dir);
    }

    fn file(self: *Staging, table: Table) *fs.File {
        return &self.files[@intFromEnum(table)];
    }

    /// Build the framed record for the current `scratch` payload and append it
    /// to the table's segment. `scratch` is cleared (retaining capacity) first.
    fn writeRecord(self: *Staging, table: Table, payload: []const u8) !void {
        var header: [8]u8 = undefined;
        std.mem.writeInt(u32, header[0..4], @intCast(payload.len), .little);
        std.mem.writeInt(u32, header[4..8], std.hash.Crc32.hash(payload), .little);

        const f = self.file(table);
        try f.writeAll(&header);
        try f.writeAll(payload);
    }

    // -- Per-table append (mirrors storage.zig insert* signatures) ----------

    pub fn appendMetrics(self: *Staging, timestamp: i64, metrics: SystemMetrics) !void {
        self.scratch.clearRetainingCapacity();
        const a = self.allocator;
        const buf = &self.scratch;
        try putI64(buf, a, timestamp);
        try putU32(buf, a, 1); // one row per cycle
        try encodeMetrics(buf, a, metrics);
        try self.writeRecord(.metrics, buf.items);
    }

    pub fn appendProcesses(self: *Staging, timestamp: i64, procs: []const ProcessInfo) !void {
        self.scratch.clearRetainingCapacity();
        const a = self.allocator;
        const buf = &self.scratch;
        try putI64(buf, a, timestamp);
        try putU32(buf, a, @intCast(procs.len));
        for (procs) |proc| try encodeProcess(buf, a, proc);
        try self.writeRecord(.processes, buf.items);
    }

    pub fn appendDisks(self: *Staging, timestamp: i64, disks: []const DiskInfo) !void {
        self.scratch.clearRetainingCapacity();
        const a = self.allocator;
        const buf = &self.scratch;
        try putI64(buf, a, timestamp);
        try putU32(buf, a, @intCast(disks.len));
        for (disks) |disk| try encodeDisk(buf, a, disk);
        try self.writeRecord(.disks, buf.items);
    }

    pub fn appendContainers(self: *Staging, timestamp: i64, containers: []const ContainerEntry) !void {
        self.scratch.clearRetainingCapacity();
        const a = self.allocator;
        const buf = &self.scratch;
        try putI64(buf, a, timestamp);
        try putU32(buf, a, @intCast(containers.len));
        for (containers) |entry| try encodeContainer(buf, a, entry);
        try self.writeRecord(.containers, buf.items);
    }

    /// Logs are appended per-entry by the daemon today (storage.insertLog),
    /// but the staging record groups a cycle's entries into one frame. Each
    /// LogEntry carries its own timestamp, so no cycle timestamp is stored.
    pub fn appendLogs(self: *Staging, entries: []const LogEntry) !void {
        self.scratch.clearRetainingCapacity();
        const a = self.allocator;
        const buf = &self.scratch;
        try putU32(buf, a, @intCast(entries.len));
        for (entries) |entry| try encodeLog(buf, a, entry);
        try self.writeRecord(.logs, buf.items);
    }

    /// One `fdatasync` per table segment - call once after a cycle's appends.
    /// Crash before this returns may lose the just-appended (uncommitted)
    /// records; everything synced previously survives.
    pub fn sync(self: *Staging) !void {
        for (&self.files) |*f| try f.sync();
    }

    /// Current on-disk size of a table's segment (header byte + all records).
    /// Drives the roll trigger.
    pub fn byteLen(self: *Staging, table: Table) !u64 {
        const stat = try self.file(table).stat();
        return stat.size;
    }

    /// Truncate a table's segment back to a fresh, single schema_version byte.
    /// Called after a successful roll has durably produced the parquet file.
    /// The open append fd is repositioned so subsequent appends start clean.
    pub fn reset(self: *Staging, table: Table) !void {
        const f = self.file(table);
        try f.setEndPos(0);
        try f.seekTo(0);
        try f.writeAll(&[_]u8{schema_version});
        try f.sync();
    }
};

// ============================================================================
// Per-struct encoders (single source of truth, shared by Staging + roll)
// ============================================================================

fn encodeMetrics(buf: *std.ArrayList(u8), a: Allocator, m: SystemMetrics) !void {
    try putF32(buf, a, m.cpu_percent);
    try putF32(buf, a, m.cpu_user);
    try putF32(buf, a, m.cpu_system);
    try putF32(buf, a, m.cpu_iowait);
    try putU64(buf, a, m.mem_total);
    try putU64(buf, a, m.mem_used);
    try putF32(buf, a, m.mem_percent);
    try putU64(buf, a, m.swap_total);
    try putU64(buf, a, m.swap_used);
}

fn encodeProcess(buf: *std.ArrayList(u8), a: Allocator, p: ProcessInfo) !void {
    try putU32(buf, a, p.pid);
    try putStr(buf, a, p.name);
    try putStr(buf, a, p.cmdline);
    try buf.append(a, p.state);
    try putF32(buf, a, p.cpu_percent);
    try putU64(buf, a, p.mem_rss);
    try putU32(buf, a, p.threads);
    try putStr(buf, a, p.username);
    try putU64(buf, a, p.io_read_bytes);
    try putU64(buf, a, p.io_write_bytes);
    try putStr(buf, a, p.cgroup);
    try putStr(buf, a, p.unit);
}

fn encodeDisk(buf: *std.ArrayList(u8), a: Allocator, d: DiskInfo) !void {
    try putStr(buf, a, d.mount_point);
    try putStr(buf, a, d.filesystem);
    try putU64(buf, a, d.total_bytes);
    try putU64(buf, a, d.used_bytes);
    try putF32(buf, a, d.percent);
}

fn encodeContainer(buf: *std.ArrayList(u8), a: Allocator, e: ContainerEntry) !void {
    try putU32(buf, a, e.vmid);
    try putStr(buf, a, e.name);
    try putStr(buf, a, e.node);
    try putStr(buf, a, e.type);
    try putStr(buf, a, e.status);
    try putU64(buf, a, e.maxmem);
    try putF64(buf, a, e.maxcpu);
    try putU64(buf, a, e.uptime);
}

fn encodeLog(buf: *std.ArrayList(u8), a: Allocator, e: LogEntry) !void {
    try putI64(buf, a, e.timestamp);
    try putStr(buf, a, e.source);
    try putOptStr(buf, a, e.unit);
    try putOptStr(buf, a, e.identifier);
    try putOptStr(buf, a, e.systemd_unit);
    try buf.append(a, e.priority);
    try putStr(buf, a, e.message);
    try putOptU32(buf, a, e.pid);
}

// ============================================================================
// Per-struct decoders
// ============================================================================

fn decodeMetrics(cur: *Cursor) !SystemMetrics {
    return SystemMetrics{
        .cpu_percent = try cur.f32_(),
        .cpu_user = try cur.f32_(),
        .cpu_system = try cur.f32_(),
        .cpu_iowait = try cur.f32_(),
        .mem_total = try cur.u64_(),
        .mem_used = try cur.u64_(),
        .mem_percent = try cur.f32_(),
        .swap_total = try cur.u64_(),
        .swap_used = try cur.u64_(),
    };
}

fn decodeProcess(cur: *Cursor, a: Allocator) !ProcessInfo {
    const pid = try cur.u32_();
    const name = try cur.str(a);
    errdefer a.free(name);
    const cmdline = try cur.str(a);
    errdefer a.free(cmdline);
    const state = (try cur.need(1))[0];
    const cpu_percent = try cur.f32_();
    const mem_rss = try cur.u64_();
    const threads = try cur.u32_();
    const username = try cur.str(a);
    errdefer a.free(username);
    const io_read_bytes = try cur.u64_();
    const io_write_bytes = try cur.u64_();
    const cgroup = try cur.str(a);
    errdefer a.free(cgroup);
    const unit = try cur.str(a);
    errdefer a.free(unit);
    return ProcessInfo{
        .pid = pid,
        .name = name,
        .cmdline = cmdline,
        .state = state,
        .cpu_percent = cpu_percent,
        .mem_rss = mem_rss,
        .threads = threads,
        .username = username,
        .io_read_bytes = io_read_bytes,
        .io_write_bytes = io_write_bytes,
        .cgroup = cgroup,
        .unit = unit,
    };
}

fn decodeDisk(cur: *Cursor, a: Allocator) !DiskInfo {
    const mount_point = try cur.str(a);
    errdefer a.free(mount_point);
    const filesystem = try cur.str(a);
    errdefer a.free(filesystem);
    return DiskInfo{
        .mount_point = mount_point,
        .filesystem = filesystem,
        .total_bytes = try cur.u64_(),
        .used_bytes = try cur.u64_(),
        .percent = try cur.f32_(),
    };
}

fn decodeContainer(cur: *Cursor, a: Allocator) !ContainerEntry {
    const vmid = try cur.u32_();
    const name = try cur.str(a);
    errdefer a.free(name);
    const node = try cur.str(a);
    errdefer a.free(node);
    const type_ = try cur.str(a);
    errdefer a.free(type_);
    const status = try cur.str(a);
    errdefer a.free(status);
    return ContainerEntry{
        .vmid = vmid,
        .name = name,
        .node = node,
        .type = type_,
        .status = status,
        .maxmem = try cur.u64_(),
        .maxcpu = try cur.f64_(),
        .uptime = try cur.u64_(),
    };
}

fn decodeLog(cur: *Cursor, a: Allocator) !LogEntry {
    const timestamp = try cur.i64_();
    const source = try cur.str(a);
    errdefer a.free(source);
    const unit = try cur.optStr(a);
    errdefer if (unit) |u| a.free(u);
    const identifier = try cur.optStr(a);
    errdefer if (identifier) |id| a.free(id);
    const systemd_unit = try cur.optStr(a);
    errdefer if (systemd_unit) |su| a.free(su);
    const priority = (try cur.need(1))[0];
    const message = try cur.str(a);
    errdefer a.free(message);
    const pid = try cur.optU32();
    return LogEntry{
        .timestamp = timestamp,
        .source = source,
        .unit = unit,
        .identifier = identifier,
        .systemd_unit = systemd_unit,
        .priority = priority,
        .message = message,
        .pid = pid,
    };
}

// ============================================================================
// StagingReader (replay / snapshot)
// ============================================================================

/// Decoded contents of one table's segment. Owns every heap allocation in
/// `cycles`; free the whole thing with deinit.
pub const Snapshot = struct {
    allocator: Allocator,
    table: Table,
    /// One entry per surviving cycle record, in append order.
    cycles: []DecodedCycle,

    pub fn deinit(self: *Snapshot) void {
        const a = self.allocator;
        for (self.cycles) |cycle| {
            switch (cycle) {
                .metrics => |rows| a.free(rows),
                .processes => |ts| {
                    for (ts.rows) |*p| freeProcess(a, p);
                    a.free(ts.rows);
                },
                .disks => |ts| {
                    for (ts.rows) |*d| freeDisk(a, d);
                    a.free(ts.rows);
                },
                .containers => |ts| {
                    for (ts.rows) |*e| e.deinit(a);
                    a.free(ts.rows);
                },
                .logs => |rows| {
                    for (rows) |*e| e.deinit(a);
                    a.free(rows);
                },
            }
        }
        a.free(self.cycles);
    }

    /// Total decoded rows across all surviving cycles. Handy for tests and the
    /// query snapshot loader.
    pub fn rowCount(self: *const Snapshot) usize {
        var total: usize = 0;
        for (self.cycles) |cycle| {
            total += switch (cycle) {
                .metrics => |rows| rows.len,
                .processes => |ts| ts.rows.len,
                .disks => |ts| ts.rows.len,
                .containers => |ts| ts.rows.len,
                .logs => |rows| rows.len,
            };
        }
        return total;
    }
};

pub fn freeProcess(a: Allocator, p: *ProcessInfo) void {
    a.free(p.name);
    a.free(p.cmdline);
    a.free(p.username);
    a.free(p.cgroup);
    a.free(p.unit);
}

pub fn freeDisk(a: Allocator, d: *DiskInfo) void {
    a.free(d.mount_point);
    a.free(d.filesystem);
}

pub const StagingReader = struct {
    /// Read and decode a table's segment from `<root>/_staging/<table>.log`.
    /// Stops at the first short read or CRC mismatch (a torn tail from a crash
    /// mid-append); every fully-committed record before it is returned. A
    /// missing segment yields an empty snapshot.
    pub fn read(allocator: Allocator, root_dir: []const u8, table: Table) !Snapshot {
        const path = try segmentPath(allocator, root_dir, table);
        defer allocator.free(path);

        const f = fs.openFileAbsolute(path, .{}) catch |err| switch (err) {
            error.FileNotFound => return Snapshot{ .allocator = allocator, .table = table, .cycles = &.{} },
            else => return err,
        };
        defer f.close();

        const data = try f.readToEndAlloc(allocator, std.math.maxInt(usize));
        defer allocator.free(data);

        return decodeSegment(allocator, table, data);
    }

    /// Decode a raw segment buffer (header byte + framed records). Exposed for
    /// tests that build segments in memory.
    pub fn decodeSegment(allocator: Allocator, table: Table, data: []const u8) !Snapshot {
        var cycles = std.ArrayList(DecodedCycle){};
        errdefer {
            var snap = Snapshot{ .allocator = allocator, .table = table, .cycles = cycles.items };
            snap.deinit();
        }

        // An empty file (not even a schema_version byte yet) is a valid empty
        // segment - the writer creates the byte, but a crash between create and
        // first write could leave it zero-length.
        if (data.len == 0) {
            return Snapshot{ .allocator = allocator, .table = table, .cycles = &.{} };
        }

        const ver = data[0];
        if (ver != schema_version) return StagingError.UnsupportedSchemaVersion;

        var pos: usize = 1;
        while (pos + 8 <= data.len) {
            const payload_len = std.mem.readInt(u32, data[pos..][0..4], .little);
            const want_crc = std.mem.readInt(u32, data[pos + 4 ..][0..4], .little);
            const body_start = pos + 8;
            const body_end = body_start + payload_len;
            // Torn tail: the declared payload runs past EOF. Stop here.
            if (body_end > data.len) break;
            const payload = data[body_start..body_end];
            // Corrupt tail: CRC mismatch. Stop here; prior records survive.
            if (std.hash.Crc32.hash(payload) != want_crc) break;

            const cycle = try decodeCycle(allocator, table, payload);
            try cycles.append(allocator, cycle);
            pos = body_end;
        }

        return Snapshot{
            .allocator = allocator,
            .table = table,
            .cycles = try cycles.toOwnedSlice(allocator),
        };
    }
};

/// Smallest possible encoded size of one row for a table - all fixed fields plus
/// the length prefixes of every string at zero length (see the `encode*` fns).
/// Used to reject a crafted/corrupt `n` before it drives a giant allocation: a
/// CRC is forgeable (not a MAC), so a torn or malicious record could declare
/// n = 0xFFFFFFFF and force a multi-hundred-GB `alloc`. Bounding n by
/// remaining_bytes / min_row_size caps the alloc at the payload's real size.
fn minRowSize(table: Table) usize {
    return switch (table) {
        // 4 f32 + mem_total/mem_used u64 + mem_percent f32 + swap u64 x2
        .metrics => 4 * 4 + 8 + 8 + 4 + 8 + 8,
        // pid u32, name/cmdline len, state u8, cpu f32, mem_rss u64, threads u32,
        // username len, io u64 x2, cgroup/unit len
        .processes => 4 + 4 + 4 + 1 + 4 + 8 + 4 + 4 + 8 + 8 + 4 + 4,
        // mount/fs len, total/used u64, percent f32
        .disks => 4 + 4 + 8 + 8 + 4,
        // vmid u32, name/node/type/status len, maxmem u64, maxcpu f64, uptime u64
        .containers => 4 + 4 + 4 + 4 + 4 + 8 + 8 + 8,
        // ts i64, source len, unit/identifier/systemd_unit opt flag x3, priority u8,
        // message len, pid opt flag
        .logs => 8 + 4 + 1 + 1 + 1 + 1 + 4 + 1,
    };
}

/// Reject a per-cycle row count that cannot fit in the bytes left in the payload
/// (a crafted/corrupt segment forcing an unbounded alloc). `cur` is positioned
/// just past the count field, so its remaining bytes bound how many rows of the
/// table's minimum encoded size could possibly follow.
fn checkRowCount(cur: *const Cursor, table: Table, n: u32) !void {
    const remaining = cur.buf.len - cur.pos;
    const max_rows = remaining / minRowSize(table);
    if (n > max_rows) return StagingError.CorruptSegment;
}

fn decodeCycle(allocator: Allocator, table: Table, payload: []const u8) !DecodedCycle {
    var cur = Cursor{ .buf = payload };
    switch (table) {
        .metrics => {
            const ts = try cur.i64_();
            const n = try cur.u32_();
            try checkRowCount(&cur, table, n);
            const rows = try allocator.alloc(MetricsRow, n);
            errdefer allocator.free(rows);
            for (rows) |*row| row.* = .{ .timestamp = ts, .metrics = try decodeMetrics(&cur) };
            return .{ .metrics = rows };
        },
        .processes => {
            const ts = try cur.i64_();
            const n = try cur.u32_();
            try checkRowCount(&cur, table, n);
            const rows = try allocator.alloc(ProcessInfo, n);
            errdefer allocator.free(rows);
            var filled: usize = 0;
            errdefer for (rows[0..filled]) |*p| freeProcess(allocator, p);
            for (rows) |*row| {
                row.* = try decodeProcess(&cur, allocator);
                filled += 1;
            }
            return .{ .processes = .{ .timestamp = ts, .rows = rows } };
        },
        .disks => {
            const ts = try cur.i64_();
            const n = try cur.u32_();
            try checkRowCount(&cur, table, n);
            const rows = try allocator.alloc(DiskInfo, n);
            errdefer allocator.free(rows);
            var filled: usize = 0;
            errdefer for (rows[0..filled]) |*d| freeDisk(allocator, d);
            for (rows) |*row| {
                row.* = try decodeDisk(&cur, allocator);
                filled += 1;
            }
            return .{ .disks = .{ .timestamp = ts, .rows = rows } };
        },
        .containers => {
            const ts = try cur.i64_();
            const n = try cur.u32_();
            try checkRowCount(&cur, table, n);
            const rows = try allocator.alloc(ContainerEntry, n);
            errdefer allocator.free(rows);
            var filled: usize = 0;
            errdefer for (rows[0..filled]) |*e| e.deinit(allocator);
            for (rows) |*row| {
                row.* = try decodeContainer(&cur, allocator);
                filled += 1;
            }
            return .{ .containers = .{ .timestamp = ts, .rows = rows } };
        },
        .logs => {
            const n = try cur.u32_();
            try checkRowCount(&cur, table, n);
            const rows = try allocator.alloc(LogEntry, n);
            errdefer allocator.free(rows);
            var filled: usize = 0;
            errdefer for (rows[0..filled]) |*e| e.deinit(allocator);
            for (rows) |*row| {
                row.* = try decodeLog(&cur, allocator);
                filled += 1;
            }
            return .{ .logs = rows };
        },
    }
}

// ============================================================================
// Path + file helpers
// ============================================================================

/// `<root>/_staging/<table>.log`. Caller owns the returned path.
pub fn segmentPath(allocator: Allocator, root_dir: []const u8, table: Table) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}/{s}/{s}.log", .{ root_dir, staging_subdir, table.name() });
}

/// `<root>/_staging/.roll.lock` - the cross-process advisory lock that
/// serializes a roll's publish+reset against a query's enumerate+snapshot. The
/// roll takes LOCK_EX, the query LOCK_SH, around their respective critical
/// sections so a query never observes the published-but-not-yet-reset window.
/// Caller owns the returned path.
pub fn rollLockPath(allocator: Allocator, root_dir: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}/{s}/.roll.lock", .{ root_dir, staging_subdir });
}

/// Open (creating if missing, 0600) the roll lockfile. Caller closes the fd. The
/// file content is never read; only its flock state matters. The `_staging` dir
/// is created (0700) if absent so a query can take the lock before any segment
/// has been written.
pub fn openRollLock(root_dir: []const u8, allocator: Allocator) !fs.File {
    const path = try rollLockPath(allocator, root_dir);
    defer allocator.free(path);
    if (fs.openFileAbsolute(path, .{ .mode = .read_write })) |f| {
        return f;
    } else |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    }
    const staging_dir = try std.fs.path.join(allocator, &.{ root_dir, staging_subdir });
    defer allocator.free(staging_dir);
    try makeDirAbsoluteIfAbsent(staging_dir);
    return fs.createFileAbsolute(path, .{ .mode = segment_mode, .read = true, .truncate = false });
}

// Directories holding segments must be 0700 (not the default 0755): their names
// and mtimes would otherwise leak table names + collection activity timing to a
// local unprivileged user, defeating the 0600 file mode. Mirrors the 0600 chmod
// storage.zig applies to the DuckDB file.
const dir_mode: fs.File.Mode = 0o700;

fn makeDirAbsoluteIfAbsent(path: []const u8) !void {
    fs.makeDirAbsolute(path) catch |err| switch (err) {
        error.PathAlreadyExists => return,
        else => return err,
    };
    chmodDir(path);
}

/// chmod a directory to 0700 best-effort (mirrors storage.zig's chmod-or-warn).
/// Uses the path-based chmod syscall: an O_RDONLY directory fd cannot be
/// fchmod'd on Linux (EBADF), so File.chmod is not usable here.
fn chmodDir(path: []const u8) void {
    chmodPath0700(path);
}

/// Best-effort `chmod(path, 0700)` via the raw syscall (same direct-syscall
/// idiom roll.zig uses for directory fsync). Logs and continues on failure.
fn chmodPath0700(path: []const u8) void {
    var buf: [fs.max_path_bytes]u8 = undefined;
    if (path.len >= buf.len) {
        std.log.warn("dir chmod skipped (path too long): {s}", .{path});
        return;
    }
    @memcpy(buf[0..path.len], path);
    buf[path.len] = 0;
    const rc = std.os.linux.chmod(@ptrCast(&buf), dir_mode);
    if (std.os.linux.E.init(rc) != .SUCCESS) {
        std.log.warn("dir chmod 0700 failed for {s}: errno {}", .{ path, std.os.linux.E.init(rc) });
    }
}

/// Open (creating if needed) a segment in append mode, mode 0600. A brand-new
/// segment gets its 1-byte schema_version written immediately.
fn openSegment(path: []const u8) !fs.File {
    // Try to open an existing segment for append first.
    if (fs.openFileAbsolute(path, .{ .mode = .read_write })) |f| {
        errdefer f.close();
        try f.seekFromEnd(0);
        return f;
    } else |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    }

    // Create fresh with the schema_version header byte.
    const f = try fs.createFileAbsolute(path, .{ .mode = segment_mode, .read = true, .truncate = true });
    errdefer f.close();
    try f.writeAll(&[_]u8{schema_version});
    return f;
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

fn testRoot(allocator: Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    return tmp.dir.realpathAlloc(allocator, ".");
}

fn sampleMetrics() SystemMetrics {
    return .{
        .cpu_percent = 12.5,
        .cpu_user = 8.0,
        .cpu_system = 4.0,
        .cpu_iowait = 0.5,
        .mem_total = 16_000_000_000,
        .mem_used = 8_000_000_000,
        .mem_percent = 50.0,
        .swap_total = 2_000_000_000,
        .swap_used = 100_000_000,
    };
}

test "staging: schema_version header present on fresh segment" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    var staging = try Staging.open(a, root);
    defer staging.deinit();

    const path = try segmentPath(a, root, .metrics);
    defer a.free(path);

    const f = try fs.openFileAbsolute(path, .{});
    defer f.close();
    var byte: [1]u8 = undefined;
    const n = try f.readAll(&byte);
    try testing.expectEqual(@as(usize, 1), n);
    try testing.expectEqual(schema_version, byte[0]);
}

test "staging: metrics round-trip" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    var staging = try Staging.open(a, root);
    defer staging.deinit();

    const m = sampleMetrics();
    try staging.appendMetrics(1000, m);
    try staging.appendMetrics(1010, m);
    try staging.sync();

    var snap = try StagingReader.read(a, root, .metrics);
    defer snap.deinit();

    try testing.expectEqual(@as(usize, 2), snap.cycles.len);
    try testing.expectEqual(@as(usize, 2), snap.rowCount());
    const row0 = snap.cycles[0].metrics[0];
    try testing.expectEqual(@as(i64, 1000), row0.timestamp);
    try testing.expectEqual(m.mem_total, row0.metrics.mem_total);
    try testing.expectApproxEqAbs(m.cpu_percent, row0.metrics.cpu_percent, 0.001);
    try testing.expectEqual(@as(i64, 1010), snap.cycles[1].metrics[0].timestamp);
}

test "staging: processes round-trip" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    var staging = try Staging.open(a, root);
    defer staging.deinit();

    const procs = [_]ProcessInfo{
        .{
            .pid = 100,
            .name = "alpha",
            .cmdline = "/bin/alpha --x",
            .state = 'R',
            .cpu_percent = 3.5,
            .mem_rss = 4096,
            .threads = 2,
            .username = "root",
            .io_read_bytes = 11,
            .io_write_bytes = 22,
            .cgroup = "/system.slice/a.service",
            .unit = "a.service",
        },
        .{
            .pid = 200,
            .name = "beta",
            .cmdline = "",
            .state = 'S',
            .cpu_percent = 0.0,
            .mem_rss = 8192,
            .threads = 1,
            .username = "dmeh",
            .io_read_bytes = 0,
            .io_write_bytes = 0,
            .cgroup = "",
            .unit = "",
        },
    };
    try staging.appendProcesses(2000, &procs);
    try staging.sync();

    var snap = try StagingReader.read(a, root, .processes);
    defer snap.deinit();

    try testing.expectEqual(@as(usize, 1), snap.cycles.len);
    const ts = snap.cycles[0].processes;
    try testing.expectEqual(@as(i64, 2000), ts.timestamp);
    try testing.expectEqual(@as(usize, 2), ts.rows.len);
    try testing.expectEqual(@as(u32, 100), ts.rows[0].pid);
    try testing.expectEqualStrings("alpha", ts.rows[0].name);
    try testing.expectEqual(@as(u8, 'R'), ts.rows[0].state);
    try testing.expectEqualStrings("a.service", ts.rows[0].unit);
    try testing.expectEqual(@as(u32, 200), ts.rows[1].pid);
    try testing.expectEqualStrings("", ts.rows[1].cmdline);
}

test "staging: disks round-trip" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    var staging = try Staging.open(a, root);
    defer staging.deinit();

    const disks = [_]DiskInfo{.{
        .mount_point = "/",
        .filesystem = "/dev/sda1",
        .total_bytes = 1_000_000,
        .used_bytes = 400_000,
        .percent = 40.0,
    }};
    try staging.appendDisks(3000, &disks);
    try staging.sync();

    var snap = try StagingReader.read(a, root, .disks);
    defer snap.deinit();

    const ts = snap.cycles[0].disks;
    try testing.expectEqual(@as(i64, 3000), ts.timestamp);
    try testing.expectEqualStrings("/", ts.rows[0].mount_point);
    try testing.expectEqualStrings("/dev/sda1", ts.rows[0].filesystem);
    try testing.expectApproxEqAbs(@as(f32, 40.0), ts.rows[0].percent, 0.001);
}

test "staging: containers round-trip" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    var staging = try Staging.open(a, root);
    defer staging.deinit();

    const containers = [_]ContainerEntry{.{
        .vmid = 101,
        .name = "web",
        .node = "aniara",
        .type = "lxc",
        .status = "running",
        .maxmem = 2_000_000_000,
        .maxcpu = 2.0,
        .uptime = 3600,
    }};
    try staging.appendContainers(4000, &containers);
    try staging.sync();

    var snap = try StagingReader.read(a, root, .containers);
    defer snap.deinit();

    const ts = snap.cycles[0].containers;
    try testing.expectEqual(@as(i64, 4000), ts.timestamp);
    try testing.expectEqual(@as(u32, 101), ts.rows[0].vmid);
    try testing.expectEqualStrings("web", ts.rows[0].name);
    try testing.expectEqualStrings("lxc", ts.rows[0].type);
    try testing.expectApproxEqAbs(@as(f64, 2.0), ts.rows[0].maxcpu, 0.001);
}

test "staging: logs round-trip with optional fields" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    var staging = try Staging.open(a, root);
    defer staging.deinit();

    const entries = [_]LogEntry{
        .{
            .timestamp = 5000,
            .source = "systemd",
            .unit = "sshd",
            .identifier = "sshd",
            .systemd_unit = "ssh.service",
            .priority = 6,
            .message = "accepted connection",
            .pid = 4321,
        },
        .{
            .timestamp = 5001,
            .source = "file",
            .unit = null,
            .identifier = null,
            .systemd_unit = null,
            .priority = 3,
            .message = "error: disk full",
            .pid = null,
        },
    };
    try staging.appendLogs(&entries);
    try staging.sync();

    var snap = try StagingReader.read(a, root, .logs);
    defer snap.deinit();

    const rows = snap.cycles[0].logs;
    try testing.expectEqual(@as(usize, 2), rows.len);
    try testing.expectEqual(@as(i64, 5000), rows[0].timestamp);
    try testing.expectEqualStrings("ssh.service", rows[0].systemd_unit.?);
    try testing.expectEqual(@as(u32, 4321), rows[0].pid.?);
    try testing.expect(rows[1].unit == null);
    try testing.expect(rows[1].pid == null);
    try testing.expectEqualStrings("error: disk full", rows[1].message);
}

test "staging: torn/corrupted tail record is skipped, survivors returned" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    {
        var staging = try Staging.open(a, root);
        defer staging.deinit();
        const m = sampleMetrics();
        try staging.appendMetrics(1000, m);
        try staging.appendMetrics(2000, m);
        try staging.sync();
    }

    // Corrupt the tail: flip the last byte of the file (inside the second
    // record's payload), which must fail the CRC for that record only.
    const path = try segmentPath(a, root, .metrics);
    defer a.free(path);
    {
        const f = try fs.openFileAbsolute(path, .{ .mode = .read_write });
        defer f.close();
        const end = try f.getEndPos();
        try f.seekTo(end - 1);
        var byte: [1]u8 = undefined;
        _ = try f.readAll(&byte);
        try f.seekTo(end - 1);
        byte[0] ^= 0xFF;
        try f.writeAll(&byte);
    }

    var snap = try StagingReader.read(a, root, .metrics);
    defer snap.deinit();

    // First (intact) record survives; the corrupted second is dropped.
    try testing.expectEqual(@as(usize, 1), snap.cycles.len);
    try testing.expectEqual(@as(i64, 1000), snap.cycles[0].metrics[0].timestamp);
}

test "staging: truncated tail (short read) is skipped" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    {
        var staging = try Staging.open(a, root);
        defer staging.deinit();
        const m = sampleMetrics();
        try staging.appendMetrics(1000, m);
        try staging.appendMetrics(2000, m);
        try staging.sync();
    }

    // Chop the last 3 bytes off so the final record's declared payload runs
    // past EOF (a torn append from power loss).
    const path = try segmentPath(a, root, .metrics);
    defer a.free(path);
    {
        const f = try fs.openFileAbsolute(path, .{ .mode = .read_write });
        defer f.close();
        const end = try f.getEndPos();
        try f.setEndPos(end - 3);
    }

    var snap = try StagingReader.read(a, root, .metrics);
    defer snap.deinit();

    try testing.expectEqual(@as(usize, 1), snap.cycles.len);
    try testing.expectEqual(@as(i64, 1000), snap.cycles[0].metrics[0].timestamp);
}

test "staging: crafted huge row count with valid CRC is rejected without giant alloc" {
    const a = testing.allocator;

    // Build a metrics segment by hand: schema_version byte, then one record
    // whose payload declares n = 0xFFFFFFFF rows but carries no row bytes. The
    // CRC is computed over the (forged) payload so the frame passes the CRC
    // check and reaches decodeCycle, exercising the unbounded-alloc guard.
    var payload = std.ArrayList(u8){};
    defer payload.deinit(a);
    try putI64(&payload, a, 1000); // cycle timestamp
    try putU32(&payload, a, 0xFFFF_FFFF); // crafted row count

    var seg = std.ArrayList(u8){};
    defer seg.deinit(a);
    try seg.append(a, schema_version);
    var header: [8]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], @intCast(payload.items.len), .little);
    std.mem.writeInt(u32, header[4..8], std.hash.Crc32.hash(payload.items), .little);
    try seg.appendSlice(a, &header);
    try seg.appendSlice(a, payload.items);

    // Using testing.allocator means a multi-GB alloc would either fail the test
    // via OOM or be caught by the leak detector; the guard must return
    // CorruptSegment before any per-row allocation happens.
    try testing.expectError(StagingError.CorruptSegment, StagingReader.decodeSegment(a, .metrics, seg.items));
}

test "staging: byteLen grows and reset truncates to header" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    var staging = try Staging.open(a, root);
    defer staging.deinit();

    const before = try staging.byteLen(.metrics);
    try testing.expectEqual(@as(u64, 1), before); // schema_version byte only

    const m = sampleMetrics();
    try staging.appendMetrics(1000, m);
    try staging.sync();
    const after = try staging.byteLen(.metrics);
    try testing.expect(after > before);

    try staging.reset(.metrics);
    try testing.expectEqual(@as(u64, 1), try staging.byteLen(.metrics));

    // After reset, the segment still decodes (empty) and appends resume.
    var snap = try StagingReader.read(a, root, .metrics);
    defer snap.deinit();
    try testing.expectEqual(@as(usize, 0), snap.cycles.len);

    try staging.appendMetrics(9999, m);
    try staging.sync();
    var snap2 = try StagingReader.read(a, root, .metrics);
    defer snap2.deinit();
    try testing.expectEqual(@as(usize, 1), snap2.cycles.len);
    try testing.expectEqual(@as(i64, 9999), snap2.cycles[0].metrics[0].timestamp);
}
