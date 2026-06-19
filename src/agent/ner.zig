//! Backend-agnostic NER (named-entity recognition) adapter interface.
//!
//! This module is PURE: it imports no C, links no native library, and names no
//! backend-specific label strings. It defines the stable daemon-side contract
//! (`Span` + `Kind`) plus a runtime-vtable `Ner` handle so redact.zig and
//! main.zig depend only on this surface. A backend (today: ner_pf.zig, an
//! FFI-GGML model; tomorrow: a pure-Zig classifier) implements the vtable and
//! exposes its OWN `init(...)` constructor returning a fully-wired `Ner`.
//!
//! Why `init` is NOT on the vtable: construction is backend-specific (the FFI
//! backend needs a gguf path + device + threads; a pure-Zig backend needs
//! entirely different args), so it cannot share one signature. The vtable
//! carries only the per-cycle hot surface (classifySpans + deinit) - exactly
//! what the redaction chokepoint depends on - so swapping backends is a
//! one-line change at the main.zig call site.
//!
//! Span offsets are HALF-OPEN [start, end) BYTE offsets into the original
//! UTF-8 text, matching redact.zig's []const u8 ranges directly (no codepoint
//! conversion). Backends MUST clamp offsets to text bounds and map their native
//! labels onto `Kind` before returning, so no backend label string ever leaks
//! up to the daemon.

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Stable daemon-side entity classes. Backends map their native labels onto
/// this enum; it survives the FFI -> pure-Zig backend swap unchanged. The
/// redact layer maps each Kind onto a placeholder Tag.
pub const Kind = enum {
    person,
    address,
    email,
    phone,
    url,
    date,
    account_number,
    secret,
    other,
};

/// One classified span: half-open [start, end) BYTE offsets, its Kind, and the
/// model's confidence score. Caller-owned once returned in a []Span.
pub const Span = struct {
    start: usize,
    end: usize,
    kind: Kind,
    score: f32,
};

pub const Error = error{
    ModelLoad,
    Inference,
    OutOfMemory,
};

/// Runtime-polymorphic NER handle. `ctx` is the backend's opaque state; the
/// vtable carries only the per-cycle hot surface the daemon depends on.
pub const Ner = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Classify `text`, returning a caller-OWNED []Span (allocated with the
        /// passed allocator). Entities scoring below `threshold` are dropped.
        /// Offsets are clamped to text bounds; labels are mapped to Kind.
        classifySpans: *const fn (
            ctx: *anyopaque,
            allocator: Allocator,
            text: []const u8,
            threshold: f32,
        ) Error![]Span,
        /// Release backend resources (e.g. unload the model). Idempotent is not
        /// required; call exactly once.
        deinit: *const fn (ctx: *anyopaque) void,
    };

    pub inline fn classifySpans(
        self: Ner,
        allocator: Allocator,
        text: []const u8,
        threshold: f32,
    ) Error![]Span {
        return self.vtable.classifySpans(self.ctx, allocator, text, threshold);
    }

    pub inline fn deinit(self: Ner) void {
        self.vtable.deinit(self.ctx);
    }
};

// ============================================================================
// Tests: a tiny stub backend proving the vtable wiring is sound WITHOUT any
// native library. This is the shape redact.zig's end-to-end test reuses.
// ============================================================================

const testing = std.testing;

/// Stub backend: returns a fixed span set so the interface can be exercised
/// with no model. Demonstrates the canonical "each backend has its own init"
/// pattern.
const StubNer = struct {
    spans: []const Span,

    fn classifySpans(
        ctx: *anyopaque,
        allocator: Allocator,
        text: []const u8,
        threshold: f32,
    ) Error![]Span {
        const self: *StubNer = @ptrCast(@alignCast(ctx));
        var list = std.ArrayList(Span){};
        errdefer list.deinit(allocator);
        for (self.spans) |s| {
            if (s.score < threshold) continue;
            // Clamp to text bounds, exactly as a real backend must.
            const start = @min(s.start, text.len);
            const end = @min(s.end, text.len);
            if (end <= start) continue;
            try list.append(allocator, .{ .start = start, .end = end, .kind = s.kind, .score = s.score });
        }
        return list.toOwnedSlice(allocator);
    }

    fn deinitFn(_: *anyopaque) void {}

    const vtable = Ner.VTable{
        .classifySpans = classifySpans,
        .deinit = deinitFn,
    };

    fn ner(self: *StubNer) Ner {
        return .{ .ctx = self, .vtable = &vtable };
    }
};

test "stub backend implements the vtable and clamps offsets" {
    var stub = StubNer{ .spans = &[_]Span{
        .{ .start = 8, .end = 16, .kind = .person, .score = 1.0 },
        .{ .start = 100, .end = 200, .kind = .address, .score = 0.9 }, // out of bounds -> clamped/dropped
    } };
    const n = stub.ner();
    const text = "Contact John Doe";
    const spans = try n.classifySpans(testing.allocator, text, 0.5);
    defer testing.allocator.free(spans);
    try testing.expectEqual(@as(usize, 1), spans.len);
    try testing.expectEqual(Kind.person, spans[0].kind);
    try testing.expectEqualStrings("John Doe", text[spans[0].start..spans[0].end]);
    n.deinit();
}

test "threshold drops low-score spans" {
    var stub = StubNer{ .spans = &[_]Span{
        .{ .start = 0, .end = 4, .kind = .person, .score = 0.3 },
    } };
    const n = stub.ner();
    const spans = try n.classifySpans(testing.allocator, "abcdef", 0.5);
    defer testing.allocator.free(spans);
    try testing.expectEqual(@as(usize, 0), spans.len);
}
