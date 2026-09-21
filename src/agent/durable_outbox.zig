//! Atomic private records. Filenames are local constants or random local IDs,
//! never server-supplied paths. A rename is not durable until the directory sync.
const std = @import("std");
pub const max_bytes = 64 * 1024 * 1024;
pub const max_records = 1024;
pub const Outbox = struct {
    dir: std.fs.Dir,
    pub fn open(root: []const u8) !Outbox {
        var base = try std.fs.cwd().openDir(root, .{});
        defer base.close();
        base.makeDir("_outbox") catch |e| if (e != error.PathAlreadyExists) return e;
        var dir = try base.openDir("_outbox", .{ .iterate = true });
        errdefer dir.close();
        try std.posix.fchmod(dir.fd, 0o700);
        return .{ .dir = dir };
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
            var it = self.dir.iterate();
            while (try it.next()) |entry| {
                if (!std.mem.startsWith(u8, entry.name, "upload-") or !std.mem.endsWith(u8, entry.name, ".json")) continue;
                const held = try std.fmt.allocPrint(a, "{s}.held", .{entry.name});
                defer a.free(held);
                try self.dir.rename(entry.name, held);
            }
            try std.posix.fsync(self.dir.fd);
            std.log.warn("ingestion binding changed: old pending uploads held locally, not sent to new binding", .{});
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
    pub fn usage(self: *Outbox) !struct { bytes: u64, records: usize } {
        var total: u64 = 0;
        var records: usize = 0;
        var it = self.dir.iterate();
        while (try it.next()) |entry| {
            if (!std.mem.startsWith(u8, entry.name, "upload-")) continue;
            const stat = self.dir.statFile(entry.name) catch |e| switch (e) {
                error.FileNotFound => continue, // Worker acknowledged concurrently.
                else => return e,
            };
            total += stat.size;
            records += 1;
        }
        return .{ .bytes = total, .records = records };
    }
    pub fn enqueue(self: *Outbox, payload: []const u8) !void {
        const used = try self.usage();
        if (used.bytes + payload.len > max_bytes or used.records >= max_records) return error.OutboxFull;
        var id: [16]u8 = undefined;
        std.crypto.random.bytes(&id);
        const hex = std.fmt.bytesToHex(id, .lower);
        var buf: [64]u8 = undefined;
        try self.write(try std.fmt.bufPrint(&buf, "upload-{s}.json", .{hex}), payload);
    }
    pub fn nextUpload(self: *Outbox, a: std.mem.Allocator) !?[]u8 {
        var it = self.dir.iterate();
        while (try it.next()) |entry| {
            if (std.mem.startsWith(u8, entry.name, "upload-") and std.mem.endsWith(u8, entry.name, ".json")) return try a.dupe(u8, entry.name);
        }
        return null;
    }
};

test "private durable record survives reopen and removal is idempotent" {
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const root = try temp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(root);
    var box = try Outbox.open(root);
    try box.write("completion.json", "{\"result\":1}");
    box.close();
    box = try Outbox.open(root);
    defer box.close();
    const data = (try box.read(std.testing.allocator, "completion.json", 1024)).?;
    defer std.testing.allocator.free(data);
    try std.testing.expectEqualStrings("{\"result\":1}", data);
    const stat = try box.dir.statFile("completion.json");
    try std.testing.expectEqual(@as(u32, 0o600), stat.mode & 0o777);
    try box.remove("completion.json");
    try box.remove("completion.json");
}

test "binding rotation holds evidence and queue exhaustion never evicts it" {
    const a = std.testing.allocator;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const root = try temp.dir.realpathAlloc(a, ".");
    defer a.free(root);
    var box = try Outbox.open(root);
    defer box.close();
    try box.bind(a, "https://example.invalid", "old-fixture-key");
    try box.enqueue("retained record");
    try box.write("claim.json", "old claim");
    try box.bind(a, "https://example.invalid", "new-fixture-key");
    try std.testing.expect((try box.nextUpload(a)) == null);
    try std.testing.expect((try box.read(a, "claim.json", 1024)) == null);
    try std.testing.expectEqual(@as(usize, 1), (try box.usage()).records);
    const full = try box.dir.createFile("upload-full.held", .{ .mode = 0o600 });
    defer full.close();
    try full.setEndPos(max_bytes); // Sparse, no large allocation.
    try std.testing.expectError(error.OutboxFull, box.enqueue("new record"));
    try std.testing.expectEqual(@as(usize, 2), (try box.usage()).records);
}
