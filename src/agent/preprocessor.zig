//! Preprocessor pipeline: the per-cycle data-transform chain the daemon runs on
//! collected ProcessInfo / DiskInfo / LogEntry BEFORE staging-append and push.
//!
//! This module is PURE and backend-agnostic: it imports only the struct
//! definitions it transforms (collector + logs) and the pure NER interface
//! (ner, for its Error set). It links NO native library and references NEITHER
//! libpf NOR ner_pf, so the base build (zig build, no -Dner) can include it with
//! lib/libpf.so absent.
//!
//! The interface mirrors ner.Ner's runtime-vtable pattern: a Preprocessor is an
//! opaque `ctx` plus a `*const VTable`. The daemon's redactable data flows
//! through TWO distinct call sites - processes+disks once per cycle, and logs
//! one entry at a time inside the drain loop - so the vtable exposes two run
//! methods (runBatch / runLog), not one fused run-all.

const std = @import("std");
const Allocator = std.mem.Allocator;
const collector = @import("collector");
const logs = @import("logs");
const ner = @import("ner");

const ProcessInfo = collector.ProcessInfo;
const DiskInfo = collector.DiskInfo;
const LogEntry = logs.LogEntry;

/// One stage in the pipeline. `ctx` is the stage's opaque state; the vtable
/// carries the two per-cycle transform surfaces plus deinit. Run methods return
/// `ner.Error!void` (NOT Allocator.Error!void) because the redaction fns they
/// dispatch to can also fail with ModelLoad / Inference.
pub const Preprocessor = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        name: []const u8,
        runBatch: *const fn (
            ctx: *anyopaque,
            allocator: Allocator,
            processes: []ProcessInfo,
            disks: []DiskInfo,
        ) ner.Error!void,
        runLog: *const fn (
            ctx: *anyopaque,
            allocator: Allocator,
            entry: *LogEntry,
        ) ner.Error!void,
        deinit: *const fn (ctx: *anyopaque) void,
    };

    pub inline fn runBatch(
        self: Preprocessor,
        allocator: Allocator,
        processes: []ProcessInfo,
        disks: []DiskInfo,
    ) ner.Error!void {
        return self.vtable.runBatch(self.ctx, allocator, processes, disks);
    }

    pub inline fn runLog(
        self: Preprocessor,
        allocator: Allocator,
        entry: *LogEntry,
    ) ner.Error!void {
        return self.vtable.runLog(self.ctx, allocator, entry);
    }

    pub inline fn deinit(self: Preprocessor) void {
        self.vtable.deinit(self.ctx);
    }
};

/// An ordered chain of stages. Owns its `stages` slice (freed in deinit).
pub const Pipeline = struct {
    stages: []Preprocessor,

    pub fn runBatch(
        self: Pipeline,
        allocator: Allocator,
        processes: []ProcessInfo,
        disks: []DiskInfo,
    ) ner.Error!void {
        for (self.stages) |stage| try stage.runBatch(allocator, processes, disks);
    }

    pub fn runLog(
        self: Pipeline,
        allocator: Allocator,
        entry: *LogEntry,
    ) ner.Error!void {
        for (self.stages) |stage| try stage.runLog(allocator, entry);
    }

    pub fn deinit(self: Pipeline, allocator: Allocator) void {
        for (self.stages) |stage| stage.deinit();
        allocator.free(self.stages);
    }
};

// ============================================================================
// Tests: empty-chain passthrough. A Pipeline with zero stages must leave the
// bytes byte-for-byte unchanged (pure collect-and-forward).
// ============================================================================

const testing = std.testing;

test "empty pipeline is a byte-for-byte passthrough" {
    const a = testing.allocator;

    var procs = [_]ProcessInfo{.{
        .pid = 1,
        .name = "sshd",
        .cmdline = try a.dupe(u8, "sshd: from user@example.com AKIA1234567890ABCD12"),
        .state = 'S',
        .cpu_percent = 0.0,
        .mem_rss = 0,
        .threads = 1,
        .username = "root",
        .io_read_bytes = 0,
        .io_write_bytes = 0,
        .cgroup = try a.dupe(u8, "/home/alice/x.service"),
        .unit = "ssh.service",
    }};
    defer a.free(procs[0].cmdline);
    defer a.free(procs[0].cgroup);

    var disks = [_]DiskInfo{.{
        .mount_point = try a.dupe(u8, "/home/alice/secret"),
        .filesystem = "ext4",
        .total_bytes = 0,
        .used_bytes = 0,
        .percent = 0.0,
    }};
    defer a.free(disks[0].mount_point);

    var entry = LogEntry{
        .timestamp = 1,
        .source = try a.dupe(u8, "/var/log/auth.log"),
        .unit = null,
        .identifier = null,
        .systemd_unit = null,
        .priority = 6,
        .message = try a.dupe(u8, "login for admin from 10.0.0.1 token=AKIA1234567890ABCD12"),
        .pid = null,
    };
    defer entry.deinit(a);

    const pipe = Pipeline{ .stages = &[_]Preprocessor{} };
    try pipe.runBatch(a, &procs, &disks);
    try pipe.runLog(a, &entry);

    // An empty chain must not touch any field.
    try testing.expectEqualStrings("sshd: from user@example.com AKIA1234567890ABCD12", procs[0].cmdline);
    try testing.expectEqualStrings("/home/alice/x.service", procs[0].cgroup);
    try testing.expectEqualStrings("/home/alice/secret", disks[0].mount_point);
    try testing.expectEqualStrings("login for admin from 10.0.0.1 token=AKIA1234567890ABCD12", entry.message);
    try testing.expectEqualStrings("/var/log/auth.log", entry.source);
}
