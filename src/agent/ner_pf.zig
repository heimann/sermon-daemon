//! FFI-GGML backend for the NER adapter (ner.zig).
//!
//! This is the ONLY module that links libpf and the ONLY place native label
//! strings (e.g. "private_person") exist. It loads the GGML PII model once at
//! daemon start (init) and runs one forward pass per classifySpans call. The
//! daemon never sees a backend label - this file maps every native label onto
//! the stable ner.Kind enum before returning.
//!
//! Lifetime contract honored here (see pf.h): pf_classify returns a malloc'd
//! pf_entity array whose `label` pointers point into ctx-owned memory valid
//! only until pf_free, and whose array is valid only until pf_entities_free.
//! So we map label -> Kind and copy start/end/score into an owned []Span BEFORE
//! calling pf_entities_free. Retaining the const char* upward would be a
//! use-after-free; the Kind enum enforces the copy by construction.
//!
//! Thread-safety: pf_ctx is NOT safe for concurrent pf_classify (the gallocr
//! forward-pass buffer is reused without locks). The daemon holds ONE handle
//! and redacts single-threaded at one chokepoint, so this is satisfied today.
//! Do not share this handle across threads without serializing.

const std = @import("std");
const Allocator = std.mem.Allocator;
const ner = @import("ner");

const Ner = ner.Ner;
const Span = ner.Span;
const Kind = ner.Kind;
const Error = ner.Error;

const c = @cImport(@cInclude("pf.h"));

/// Default device passed to pf_load. "cpu" keeps the edge build off the GPU.
const default_device = "cpu";

/// Backend state: the model handle plus the allocator used to heap-allocate
/// this struct (so deinit can free it).
const PfNer = struct {
    handle: *c.pf_ctx,
    alloc: Allocator,

    const vtable = Ner.VTable{
        .classifySpans = classifySpans,
        .deinit = deinitFn,
    };

    fn classifySpans(
        ctx: *anyopaque,
        allocator: Allocator,
        text: []const u8,
        threshold: f32,
    ) Error![]Span {
        const self: *PfNer = @ptrCast(@alignCast(ctx));

        var out: [*c]c.pf_entity = null;
        var n_out: usize = 0;
        const rc = c.pf_classify(
            self.handle,
            text.ptr,
            text.len,
            threshold,
            &out,
            &n_out,
        );
        if (rc != 0) return Error.Inference;
        // out may be null with n_out==0 on a clean "no entities" result.
        defer if (out != null) c.pf_entities_free(out, n_out);

        var list = std.ArrayList(Span){};
        errdefer list.deinit(allocator);

        var i: usize = 0;
        while (i < n_out) : (i += 1) {
            const ent = out[i];
            // Clamp byte offsets to text bounds (the backend reports offsets
            // into the original UTF-8; never trust them past the buffer).
            const raw_start: usize = if (ent.start < 0) 0 else @intCast(ent.start);
            const raw_end: usize = if (ent.end < 0) 0 else @intCast(ent.end);
            const start = @min(raw_start, text.len);
            const end = @min(raw_end, text.len);
            if (end <= start) continue;

            const kind = mapLabel(ent.label);
            try list.append(allocator, .{
                .start = start,
                .end = end,
                .kind = kind,
                .score = ent.score,
            });
        }

        return list.toOwnedSlice(allocator);
    }

    fn deinitFn(ctx: *anyopaque) void {
        const self: *PfNer = @ptrCast(@alignCast(ctx));
        c.pf_free(self.handle);
        const alloc = self.alloc;
        alloc.destroy(self);
    }
};

/// Map a native model label (NUL-terminated C string) onto the stable Kind
/// enum. This is the ONLY place the daemon's contract touches backend strings.
/// Unknown labels fall through to `.other` so a model update that adds a label
/// degrades to a generic redaction rather than crashing or leaking.
fn mapLabel(label: [*c]const u8) Kind {
    if (label == null) return .other;
    const s = std.mem.span(label);
    const table = [_]struct { prefix: []const u8, kind: Kind }{
        .{ .prefix = "private_person", .kind = .person },
        .{ .prefix = "private_address", .kind = .address },
        .{ .prefix = "private_email", .kind = .email },
        .{ .prefix = "private_phone", .kind = .phone },
        .{ .prefix = "private_url", .kind = .url },
        .{ .prefix = "private_date", .kind = .date },
        .{ .prefix = "account_number", .kind = .account_number },
        .{ .prefix = "secret", .kind = .secret },
    };
    for (table) |row| {
        if (std.mem.startsWith(u8, s, row.prefix)) return row.kind;
    }
    return .other;
}

/// Load the GGML PII model and return a fully-wired Ner. `gguf_path` must be a
/// NUL-terminated path. On any failure returns Error.ModelLoad - the caller
/// (main.zig) catches this and degrades to deterministic-only redaction; the
/// daemon NEVER crashes or runs raw because the model is absent or broken.
pub fn init(alloc: Allocator, gguf_path: [:0]const u8) Error!Ner {
    // ABI guard: refuse to run against a libpf whose flat C contract drifted.
    if (c.pf_abi_version() != c.PF_ABI_VERSION) return Error.ModelLoad;

    const handle = c.pf_load(gguf_path.ptr, default_device, 0) orelse
        return Error.ModelLoad;
    errdefer c.pf_free(handle);

    const self = alloc.create(PfNer) catch return Error.ModelLoad;
    self.* = .{ .handle = handle, .alloc = alloc };

    return Ner{ .ctx = self, .vtable = &PfNer.vtable };
}

// ============================================================================
// Tests (gated on lib/libpf.so + the model being present; the build wires a
// dedicated test target that only runs when lib/libpf.so exists, and the test
// itself skips cleanly if the model file is absent).
// ============================================================================

const testing = std.testing;

/// Resolve the model path relative to the repo root at test time. The test
/// target's cwd is the project root, so the symlinked model lives here.
const test_model_path = "models/privacy-filter-f16.gguf";

fn modelPresent() bool {
    std.fs.cwd().access(test_model_path, .{}) catch return false;
    return true;
}

test "label mapping covers the native taxonomy" {
    try testing.expectEqual(Kind.person, mapLabel("private_person"));
    try testing.expectEqual(Kind.address, mapLabel("private_address"));
    try testing.expectEqual(Kind.email, mapLabel("private_email"));
    try testing.expectEqual(Kind.phone, mapLabel("private_phone"));
    try testing.expectEqual(Kind.url, mapLabel("private_url"));
    try testing.expectEqual(Kind.date, mapLabel("private_date"));
    try testing.expectEqual(Kind.account_number, mapLabel("account_number"));
    try testing.expectEqual(Kind.secret, mapLabel("secret"));
    try testing.expectEqual(Kind.other, mapLabel("brand_new_label"));
    try testing.expectEqual(Kind.other, mapLabel(null));
}

test "end-to-end: model classifies person + address in-process" {
    if (!modelPresent()) return error.SkipZigTest;

    const n = try init(testing.allocator, test_model_path);
    defer n.deinit();

    const text = "Contact John Doe at jdoe@example.com, 742 Evergreen Terrace, key AKIAIOSFODNN7EXAMPLE";
    const spans = try n.classifySpans(testing.allocator, text, 0.5);
    defer testing.allocator.free(spans);

    // The model must label a person span covering "John Doe" and an address
    // span starting at "742 Evergreen Terrace". The address span may
    // over-extend (observed in the lib smoke test); we assert it covers the
    // street address, and rely on redact.zig's merge to keep the AWS key
    // redaction intact via the deterministic scanner.
    var found_person = false;
    var found_address = false;
    for (spans) |s| {
        const slice = text[s.start..s.end];
        switch (s.kind) {
            .person => {
                if (std.mem.indexOf(u8, slice, "John Doe") != null) found_person = true;
            },
            .address => {
                if (std.mem.indexOf(u8, slice, "742 Evergreen Terrace") != null) found_address = true;
            },
            else => {},
        }
    }
    try testing.expect(found_person);
    try testing.expect(found_address);
}
