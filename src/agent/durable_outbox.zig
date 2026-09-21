//! Atomic private records. Filenames are local constants or random local IDs,
//! never server-supplied paths. A rename is not durable until the directory sync.
const std = @import("std");
pub const max_bytes = 64 * 1024 * 1024;
pub const max_records = 1024;
/// Held (old binding) and quarantined (poison) records are evidence for an
/// operator, not a delivery queue. Each has its own cap so neither can ever
/// consume the live queue's budget; the oldest is evicted first.
pub const held_max_bytes = 16 * 1024 * 1024;
pub const held_max_records = 256;
pub const bad_max_bytes = 8 * 1024 * 1024;
pub const bad_max_records = 64;
const orphan_tmp_age_ns = 60 * std.time.ns_per_s;

/// Process-wide loss diagnostics, reported in the telemetry `log_stats` block.
/// Collector and worker threads both write them.
pub const Counters = struct {
    upload_queue_dropped: std.atomic.Value(u64) = .init(0),
    spool_errors: std.atomic.Value(u64) = .init(0),
    quarantined: std.atomic.Value(u64) = .init(0),
    held_evicted: std.atomic.Value(u64) = .init(0),
};
pub var counters: Counters = .{};

/// Live-queue usage shared by every Outbox handle on one spool directory in
/// this process, so the collector never scans the spool per log line. A
/// reconcile racing an in-flight enqueue can undercount by that one record
/// until the next reconcile; the caps are budgets, not security boundaries.
pub const Shared = struct {
    mutex: std.Thread.Mutex = .{},
    bytes: u64 = 0,
    records: usize = 0,

    fn reserve(self: *Shared, len: usize) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.bytes + len > max_bytes or self.records >= max_records) return false;
        self.bytes += len;
        self.records += 1;
        return true;
    }
    fn release(self: *Shared, len: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.bytes -|= len;
        self.records -|= 1;
    }
    pub fn full(self: *Shared, next_len: usize) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.bytes + next_len > max_bytes or self.records >= max_records;
    }
};
pub const Usage = struct { bytes: u64, records: usize };

fn isLive(name: []const u8) bool {
    return std.mem.startsWith(u8, name, "upload-") and std.mem.endsWith(u8, name, ".json");
}

pub const Outbox = struct {
    dir: std.fs.Dir,
    shared: *Shared,
    pub fn open(root: []const u8, shared: *Shared) !Outbox {
        var base = try std.fs.cwd().openDir(root, .{});
        defer base.close();
        base.makeDir("_outbox") catch |e| if (e != error.PathAlreadyExists) return e;
        var dir = try base.openDir("_outbox", .{ .iterate = true });
        errdefer dir.close();
        try std.posix.fchmod(dir.fd, 0o700);
        return .{ .dir = dir, .shared = shared };
    }
    pub fn close(self: *Outbox) void {
        self.dir.close();
    }
    /// A rotated key may bind a different server. Never disclose old evidence
    /// under a new binding. Hold old uploads for explicit operator recovery.
    pub fn bind(self: *Outbox, a: std.mem.Allocator, origin: []const u8, key: []const u8) !void {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(origin);
        hash.update(&.{0});
        hash.update(key);
        const fingerprint = std.fmt.bytesToHex(hash.finalResult(), .lower);
        if (try self.read(a, "binding", 64)) |old| {
            defer a.free(old);
            if (std.mem.eql(u8, old, &fingerprint)) return;
            try self.remove("claim.json");
            try self.remove("completion.json");
            try self.remove("telemetry.json");
            try self.remove("rules.json");
            // Renaming while iterating can revisit entries; collect names first.
            var names = std.ArrayList([]u8){};
            defer {
                for (names.items) |n| a.free(n);
                names.deinit(a);
            }
            var it = self.dir.iterate();
            while (try it.next()) |entry| {
                if (isLive(entry.name)) try names.append(a, try a.dupe(u8, entry.name));
            }
            for (names.items) |name| {
                const held = try std.fmt.allocPrint(a, "{s}.held", .{name});
                defer a.free(held);
                try self.dir.rename(name, held);
            }
            try std.posix.fsync(self.dir.fd);
            std.log.warn("ingestion binding changed: old pending uploads held locally, not sent to new binding", .{});
            try self.trim(a, ".held", held_max_records, held_max_bytes, &counters.held_evicted);
            try self.reconcile();
        }
        try self.write("binding", &fingerprint);
    }
    pub fn write(self: *Outbox, name: []const u8, bytes: []const u8) !void {
        var tmp: [128]u8 = undefined;
        const path = try std.fmt.bufPrint(&tmp, "{s}.tmp", .{name});
        var f = try self.dir.createFile(path, .{ .mode = 0o600 });
        defer f.close();
        errdefer self.dir.deleteFile(path) catch {};
        try f.writeAll(bytes);
        try f.sync();
        try self.dir.rename(path, name);
        try std.posix.fsync(self.dir.fd);
    }
    pub fn remove(self: *Outbox, name: []const u8) !void {
        self.dir.deleteFile(name) catch |e| if (e != error.FileNotFound) return e;
        try std.posix.fsync(self.dir.fd);
    }
    pub fn read(self: *Outbox, a: std.mem.Allocator, name: []const u8, cap: usize) !?[]u8 {
        return self.dir.readFileAlloc(a, name, cap) catch |e| switch (e) {
            error.FileNotFound => null,
            else => return e,
        };
    }
    /// Cached live-queue usage. Never touches the filesystem.
    pub fn usage(self: *Outbox) Usage {
        self.shared.mutex.lock();
        defer self.shared.mutex.unlock();
        return .{ .bytes = self.shared.bytes, .records = self.shared.records };
    }
    /// Rebuild the cache from the directory: startup, binding change, and an
    /// occasional worker pass that repairs drift from crashes or manual cleanup.
    pub fn reconcile(self: *Outbox) !void {
        var total: u64 = 0;
        var records: usize = 0;
        var it = self.dir.iterate();
        while (try it.next()) |entry| {
            if (!isLive(entry.name)) continue;
            const stat = self.dir.statFile(entry.name) catch |e| switch (e) {
                error.FileNotFound => continue, // Worker acknowledged concurrently.
                else => return e,
            };
            total += stat.size;
            records += 1;
        }
        self.shared.mutex.lock();
        defer self.shared.mutex.unlock();
        self.shared.bytes = total;
        self.shared.records = records;
    }
    /// Startup only: temp files from an interrupted write are never live data.
    /// The age floor spares a query child that briefly outlived its parent.
    pub fn cleanOrphans(self: *Outbox) !void {
        const now = std.time.nanoTimestamp();
        var it = self.dir.iterate();
        var removed = false;
        while (try it.next()) |entry| {
            if (!std.mem.endsWith(u8, entry.name, ".tmp")) continue;
            const stat = self.dir.statFile(entry.name) catch continue;
            if (now - stat.mtime < orphan_tmp_age_ns) continue;
            self.dir.deleteFile(entry.name) catch continue;
            removed = true;
        }
        if (removed) try std.posix.fsync(self.dir.fd);
    }
    pub fn enqueue(self: *Outbox, payload: []const u8) !void {
        if (!self.shared.reserve(payload.len)) return error.OutboxFull;
        errdefer self.shared.release(payload.len);
        var id: [16]u8 = undefined;
        std.crypto.random.bytes(&id);
        const hex = std.fmt.bytesToHex(id, .lower);
        var buf: [64]u8 = undefined;
        try self.write(try std.fmt.bufPrint(&buf, "upload-{s}.json", .{hex}), payload);
    }
    /// Remove an acknowledged (or deliberately discarded) live record.
    pub fn removeUpload(self: *Outbox, name: []const u8) !void {
        const size = (self.dir.statFile(name) catch |e| switch (e) {
            error.FileNotFound => return,
            else => return e,
        }).size;
        try self.remove(name);
        self.shared.release(size);
    }
    /// Move a record that can never be delivered out of the live queue. It is
    /// kept, bounded, for local diagnosis; the retained store still has the row.
    pub fn quarantine(self: *Outbox, a: std.mem.Allocator, name: []const u8) !void {
        const size = (self.dir.statFile(name) catch |e| switch (e) {
            error.FileNotFound => return,
            else => return e,
        }).size;
        const bad = try std.fmt.allocPrint(a, "{s}.bad", .{name});
        defer a.free(bad);
        try self.dir.rename(name, bad);
        try std.posix.fsync(self.dir.fd);
        self.shared.release(size);
        _ = counters.quarantined.fetchAdd(1, .monotonic);
        try self.trim(a, ".bad", bad_max_records, bad_max_bytes, null);
    }
    fn trim(self: *Outbox, a: std.mem.Allocator, suffix: []const u8, cap_records: usize, cap_bytes: u64, evicted: ?*std.atomic.Value(u64)) !void {
        const Item = struct { name: []u8, mtime: i128, size: u64 };
        var items = std.ArrayList(Item){};
        defer {
            for (items.items) |i| a.free(i.name);
            items.deinit(a);
        }
        var total: u64 = 0;
        var it = self.dir.iterate();
        while (try it.next()) |entry| {
            if (!std.mem.startsWith(u8, entry.name, "upload-") or !std.mem.endsWith(u8, entry.name, suffix)) continue;
            const stat = self.dir.statFile(entry.name) catch continue;
            try items.append(a, .{ .name = try a.dupe(u8, entry.name), .mtime = stat.mtime, .size = stat.size });
            total += stat.size;
        }
        std.mem.sort(Item, items.items, {}, struct {
            fn older(_: void, x: Item, y: Item) bool {
                return x.mtime < y.mtime;
            }
        }.older);
        var count = items.items.len;
        var removed = false;
        for (items.items) |i| {
            if (count <= cap_records and total <= cap_bytes) break;
            self.dir.deleteFile(i.name) catch continue;
            count -= 1;
            total -= i.size;
            removed = true;
            if (evicted) |c| _ = c.fetchAdd(1, .monotonic);
        }
        if (removed) {
            try std.posix.fsync(self.dir.fd);
            std.log.warn("outbox {s} records exceeded their cap: oldest evicted", .{suffix});
        }
    }
};

/// Collector-side policy: the local store is the durable copy, so a full or
/// failing upload spool costs the upload, never collection. `offer` cannot fail.
pub const Gate = struct {
    box: ?*Outbox,
    dropping: bool = false,
    last_error_log: i64 = 0,

    pub fn offer(self: *Gate, payload: []const u8) bool {
        const box = self.box orelse return self.drop();
        box.enqueue(payload) catch |err| {
            if (err == error.OutboxFull) {
                if (!self.dropping) std.log.warn("upload outbox full: new log uploads dropped until it drains; rows remain in the local store and via retained-log queries", .{});
                self.dropping = true;
            } else {
                _ = counters.spool_errors.fetchAdd(1, .monotonic);
                const now = std.time.milliTimestamp();
                if (self.last_error_log == 0 or now - self.last_error_log >= 10 * 60 * 1000) {
                    self.last_error_log = now;
                    std.log.warn("upload spool write failed: {}; log uploads dropped, local collection continues", .{err});
                }
            }
            return self.drop();
        };
        if (self.dropping) std.log.info("upload outbox draining again: log uploads resumed", .{});
        self.dropping = false;
        return true;
    }
    fn drop(_: *Gate) bool {
        _ = counters.upload_queue_dropped.fetchAdd(1, .monotonic);
        return false;
    }
};

test "private durable record survives reopen and removal is idempotent" {
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const root = try temp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(root);
    var shared = Shared{};
    var box = try Outbox.open(root, &shared);
    try box.write("completion.json", "{\"result\":1}");
    box.close();
    box = try Outbox.open(root, &shared);
    defer box.close();
    const data = (try box.read(std.testing.allocator, "completion.json", 1024)).?;
    defer std.testing.allocator.free(data);
    try std.testing.expectEqualStrings("{\"result\":1}", data);
    const stat = try box.dir.statFile("completion.json");
    try std.testing.expectEqual(@as(u32, 0o600), stat.mode & 0o777);
    try box.remove("completion.json");
    try box.remove("completion.json");
}

test "binding rotation holds evidence without consuming the live cap" {
    const a = std.testing.allocator;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const root = try temp.dir.realpathAlloc(a, ".");
    defer a.free(root);
    var shared = Shared{};
    var box = try Outbox.open(root, &shared);
    defer box.close();
    try box.bind(a, "https://example.invalid", "old-fixture-key");
    try box.enqueue("retained record");
    try std.testing.expectEqual(@as(usize, 1), box.usage().records);
    try box.write("claim.json", "old claim");
    try box.bind(a, "https://example.invalid", "new-fixture-key");
    try std.testing.expect((try box.read(a, "claim.json", 1024)) == null);
    try std.testing.expectEqual(@as(usize, 0), box.usage().records);
    // A held backlog at its own cap must not stall new delivery.
    const full = try box.dir.createFile("upload-full.json.held", .{ .mode = 0o600 });
    try full.setEndPos(max_bytes); // Sparse, no large allocation.
    full.close();
    const orphan = try box.dir.createFile("upload-orphan.json.tmp", .{ .mode = 0o600 });
    try orphan.setEndPos(max_bytes);
    orphan.close();
    try box.reconcile();
    try box.enqueue("new record");
    try std.testing.expectEqual(@as(usize, 1), box.usage().records);
    try std.testing.expectEqual(@as(u64, "new record".len), box.usage().bytes);
}

test "held and quarantined records are bounded, oldest evicted first" {
    const a = std.testing.allocator;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const root = try temp.dir.realpathAlloc(a, ".");
    defer a.free(root);
    var shared = Shared{};
    var box = try Outbox.open(root, &shared);
    defer box.close();
    const before = counters.held_evicted.load(.monotonic);
    var buf: [64]u8 = undefined;
    for (0..held_max_records + 3) |i| {
        const f = try box.dir.createFile(try std.fmt.bufPrint(&buf, "upload-{d:0>4}.json.held", .{i}), .{ .mode = 0o600 });
        defer f.close();
        const t: i128 = @as(i128, @intCast(i + 1)) * std.time.ns_per_s;
        try f.updateTimes(t, t);
    }
    try box.trim(a, ".held", held_max_records, held_max_bytes, &counters.held_evicted);
    try std.testing.expectEqual(before + 3, counters.held_evicted.load(.monotonic));
    try std.testing.expectError(error.FileNotFound, box.dir.statFile("upload-0000.json.held"));
    _ = try box.dir.statFile("upload-0003.json.held");

    for (0..bad_max_records + 2) |_| try box.enqueue("poison");
    var names = std.ArrayList([]u8){};
    defer {
        for (names.items) |n| a.free(n);
        names.deinit(a);
    }
    var it = box.dir.iterate();
    while (try it.next()) |e| if (isLive(e.name)) try names.append(a, try a.dupe(u8, e.name));
    for (names.items) |n| try box.quarantine(a, n);
    try std.testing.expectEqual(@as(usize, 0), box.usage().records);
    var bad: usize = 0;
    it = box.dir.iterate();
    while (try it.next()) |e| bad += @intFromBool(std.mem.endsWith(u8, e.name, ".bad"));
    try std.testing.expectEqual(@as(usize, bad_max_records), bad);
}

test "usage is cached, reconciled from disk, and startup removes stale temp files" {
    const a = std.testing.allocator;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const root = try temp.dir.realpathAlloc(a, ".");
    defer a.free(root);
    var shared = Shared{};
    var box = try Outbox.open(root, &shared);
    defer box.close();
    try box.enqueue("one");
    try box.enqueue("three");
    var fresh = Shared{};
    var reopened = try Outbox.open(root, &fresh);
    defer reopened.close();
    try std.testing.expectEqual(@as(usize, 0), reopened.usage().records);
    try reopened.reconcile();
    try std.testing.expectEqual(Usage{ .bytes = 8, .records = 2 }, reopened.usage());
    const name = (try nextLive(&reopened, a)).?;
    defer a.free(name);
    try reopened.removeUpload(name);
    try reopened.removeUpload(name); // Idempotent: never double-releases.
    try std.testing.expectEqual(@as(usize, 1), reopened.usage().records);

    const stale = try box.dir.createFile("upload-stale.json.tmp", .{ .mode = 0o600 });
    try stale.updateTimes(0, 0);
    stale.close();
    const recent = try box.dir.createFile("completion.json.tmp", .{ .mode = 0o600 });
    recent.close();
    try box.cleanOrphans();
    try std.testing.expectError(error.FileNotFound, box.dir.statFile("upload-stale.json.tmp"));
    _ = try box.dir.statFile("completion.json.tmp");
}

fn nextLive(box: *Outbox, a: std.mem.Allocator) !?[]u8 {
    var it = box.dir.iterate();
    while (try it.next()) |entry| if (isLive(entry.name)) return try a.dupe(u8, entry.name);
    return null;
}

test "a full or failing spool drops the upload and never fails the collector" {
    const a = std.testing.allocator;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const root = try temp.dir.realpathAlloc(a, ".");
    defer a.free(root);
    var shared = Shared{};
    var box = try Outbox.open(root, &shared);
    defer box.close();
    var gate = Gate{ .box = &box };
    const dropped = counters.upload_queue_dropped.load(.monotonic);
    const errors = counters.spool_errors.load(.monotonic);
    try std.testing.expect(gate.offer("row"));
    shared.records = max_records;
    try std.testing.expect(!gate.offer("row"));
    try std.testing.expect(!gate.offer("row"));
    try std.testing.expect(gate.dropping);
    try std.testing.expectEqual(dropped + 2, counters.upload_queue_dropped.load(.monotonic));
    shared.records = 1;
    try std.testing.expect(gate.offer("row"));
    try std.testing.expect(!gate.dropping);
    // ENOSPC, EIO, a removed spool directory: all degrade to the drop path.
    try temp.dir.deleteTree("_outbox");
    try std.testing.expect(!gate.offer("row"));
    try std.testing.expectEqual(errors + 1, counters.spool_errors.load(.monotonic));
    try std.testing.expectEqual(dropped + 3, counters.upload_queue_dropped.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 2), box.usage().records);
    var absent = Gate{ .box = null };
    try std.testing.expect(!absent.offer("row"));
}
