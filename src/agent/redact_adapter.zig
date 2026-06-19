//! Preprocessor adapters that wrap the existing redaction layer.
//!
//! Both adapters are PURE-Zig and link NOTHING native: they import only the
//! pipeline interface, the struct definitions, and the existing redact module
//! (which itself only depends on the pure `ner` interface, never ner_pf). The
//! leak-fixed clip-not-drop merge (redact.redactTextMerged) is preserved exactly
//! because these adapters only DISPATCH to the unchanged redact_mod fns - no
//! merge logic is duplicated here.
//!
//!   * DeterministicRedact: stateless singleton. Runs the byte-scanners only
//!     (passes null for the NER backend). asPreprocessor() returns a handle
//!     whose ctx is a sentinel and whose vtable is a file-scope const.
//!   * NerRedact: heap struct holding a pure ner.Ner backend handle. Dispatches
//!     to the same redact fns WITH the backend. It imports ONLY the pure ner
//!     interface, so this module stays linkable with libpf absent. The concrete
//!     ner_pf backend is constructed elsewhere (main.zig loadNer) and injected.

const std = @import("std");
const Allocator = std.mem.Allocator;
const preprocessor = @import("preprocessor");
const collector = @import("collector");
const logs = @import("logs");
const redact = @import("redact");
const ner = @import("ner");

const Preprocessor = preprocessor.Preprocessor;
const ProcessInfo = collector.ProcessInfo;
const DiskInfo = collector.DiskInfo;
const LogEntry = logs.LogEntry;

// ============================================================================
// DeterministicRedact: byte-scanners only, no NER backend, no native deps.
// ============================================================================

pub const DeterministicRedact = struct {
    fn runBatch(
        _: *anyopaque,
        allocator: Allocator,
        processes: []ProcessInfo,
        disks: []DiskInfo,
    ) ner.Error!void {
        try redact.redactProcesses(allocator, processes, null);
        try redact.redactDisks(allocator, disks);
    }

    fn runLog(
        _: *anyopaque,
        allocator: Allocator,
        entry: *LogEntry,
    ) ner.Error!void {
        try redact.redactLog(allocator, entry, null);
    }

    fn deinitFn(_: *anyopaque) void {}

    const vtable = Preprocessor.VTable{
        .name = "redact",
        .runBatch = runBatch,
        .runLog = runLog,
        .deinit = deinitFn,
    };

    // Stateless: ctx is never dereferenced, but must be a valid non-null pointer
    // so @ptrCast in any future stateful variant stays sound. Use a sentinel.
    var sentinel: u8 = 0;

    pub fn asPreprocessor() Preprocessor {
        return .{ .ctx = &sentinel, .vtable = &vtable };
    }
};

// ============================================================================
// NerRedact: deterministic scanners + model-backed NER merge. Owns the backend.
// ============================================================================

pub const NerRedact = struct {
    ner_backend: ner.Ner,

    fn runBatch(
        ctx: *anyopaque,
        allocator: Allocator,
        processes: []ProcessInfo,
        disks: []DiskInfo,
    ) ner.Error!void {
        const self: *NerRedact = @ptrCast(@alignCast(ctx));
        try redact.redactProcesses(allocator, processes, self.ner_backend);
        try redact.redactDisks(allocator, disks);
    }

    fn runLog(
        ctx: *anyopaque,
        allocator: Allocator,
        entry: *LogEntry,
    ) ner.Error!void {
        const self: *NerRedact = @ptrCast(@alignCast(ctx));
        try redact.redactLog(allocator, entry, self.ner_backend);
    }

    fn deinitFn(ctx: *anyopaque) void {
        const self: *NerRedact = @ptrCast(@alignCast(ctx));
        self.ner_backend.deinit();
    }

    const vtable = Preprocessor.VTable{
        .name = "ner",
        .runBatch = runBatch,
        .runLog = runLog,
        .deinit = deinitFn,
    };

    pub fn asPreprocessor(self: *NerRedact) Preprocessor {
        return .{ .ctx = self, .vtable = &vtable };
    }
};

// ============================================================================
// Tests: deterministic scrubbing through the pipeline, no native deps.
// ============================================================================

const Pipeline = preprocessor.Pipeline;
const testing = std.testing;

test "deterministic stage scrubs structured secrets through the pipeline" {
    const a = testing.allocator;

    var procs = [_]ProcessInfo{.{
        .pid = 42,
        .name = "postgres",
        .cmdline = try a.dupe(u8, "psql --password=hunter2 host 10.0.0.5 user@corp.com"),
        .state = 'R',
        .cpu_percent = 1.0,
        .mem_rss = 0,
        .threads = 1,
        .username = "postgres",
        .io_read_bytes = 0,
        .io_write_bytes = 0,
        .cgroup = try a.dupe(u8, "/system.slice/sshd.service"),
        .unit = "postgresql.service",
    }};
    defer a.free(procs[0].cmdline);
    defer a.free(procs[0].cgroup);

    var disks = [_]DiskInfo{.{
        .mount_point = try a.dupe(u8, "/home/alice/data"),
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
        .message = try a.dupe(u8, "auth ssn 123-45-6789 card 4111 1111 1111 1111 ip 10.0.0.1"),
        .pid = null,
    };
    defer entry.deinit(a);

    var stages = [_]Preprocessor{DeterministicRedact.asPreprocessor()};
    const pipe = Pipeline{ .stages = &stages };

    try pipe.runBatch(a, &procs, &disks);
    try pipe.runLog(a, &entry);

    // cmdline: password value, IP, and email all scrubbed; name preserved.
    try testing.expect(std.mem.indexOf(u8, procs[0].cmdline, "hunter2") == null);
    try testing.expect(std.mem.indexOf(u8, procs[0].cmdline, "10.0.0.5") == null);
    try testing.expect(std.mem.indexOf(u8, procs[0].cmdline, "corp.com") == null);
    try testing.expectEqualStrings("postgres", procs[0].name);

    // disk mount point home user redacted, structure kept.
    try testing.expectEqualStrings("/home/<REDACTED:DIR>/data", disks[0].mount_point);

    // log: ssn, card, ip all scrubbed; surrounding words intact (shape-preserving).
    try testing.expect(std.mem.indexOf(u8, entry.message, "123-45-6789") == null);
    try testing.expect(std.mem.indexOf(u8, entry.message, "4111 1111 1111 1111") == null);
    try testing.expect(std.mem.indexOf(u8, entry.message, "10.0.0.1") == null);
    try testing.expect(std.mem.indexOf(u8, entry.message, "<REDACTED:SSN>") != null);
    try testing.expect(std.mem.indexOf(u8, entry.message, "<REDACTED:CARD>") != null);
    try testing.expect(std.mem.indexOf(u8, entry.message, "<REDACTED:IP>") != null);
    try testing.expect(std.mem.indexOf(u8, entry.message, "auth") != null);
}

test "ner stage merges a PERSON the deterministic scanners miss" {
    const a = testing.allocator;

    // Stub backend: classifies a fixed PERSON span the byte-scanners cannot
    // characterize (a bare name). Proves the NER path is reachable through the
    // pipeline WITHOUT loading a real model (model-free guarantee).
    const Stub = struct {
        span: ner.Span,
        fn classify(ctx: *anyopaque, alloc: Allocator, text: []const u8, threshold: f32) ner.Error![]ner.Span {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            _ = threshold;
            const start = @min(self.span.start, text.len);
            const end = @min(self.span.end, text.len);
            if (end <= start) return alloc.alloc(ner.Span, 0);
            const out = try alloc.alloc(ner.Span, 1);
            out[0] = .{ .start = start, .end = end, .kind = self.span.kind, .score = self.span.score };
            return out;
        }
        fn deinitFn(_: *anyopaque) void {}
        const vt = ner.Ner.VTable{ .classifySpans = classify, .deinit = deinitFn };
    };

    // "user Jane Roe logged in": offsets of "Jane Roe" -> [5, 13)
    var stub = Stub{ .span = .{ .start = 5, .end = 13, .kind = .person, .score = 1.0 } };
    const backend = ner.Ner{ .ctx = &stub, .vtable = &Stub.vt };

    const nr = try a.create(NerRedact);
    nr.* = .{ .ner_backend = backend };

    var entry = LogEntry{
        .timestamp = 1,
        .source = try a.dupe(u8, "/var/log/app.log"),
        .unit = null,
        .identifier = null,
        .systemd_unit = null,
        .priority = 6,
        .message = try a.dupe(u8, "user Jane Roe logged in"),
        .pid = null,
    };
    defer entry.deinit(a);

    var stages = [_]Preprocessor{nr.asPreprocessor()};
    const pipe = Pipeline{ .stages = &stages };
    // stages is a stack array, so we don't call pipe.deinit (it would free a
    // non-heap slice). Tear down the one stage and free the heap struct directly.
    defer a.destroy(nr);
    defer nr.asPreprocessor().deinit(); // stub backend deinit is a no-op

    try pipe.runLog(a, &entry);

    try testing.expect(std.mem.indexOf(u8, entry.message, "Jane Roe") == null);
    try testing.expect(std.mem.indexOf(u8, entry.message, "<REDACTED:PERSON>") != null);
}
