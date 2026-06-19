//! Pure-Zig config + store-gate tests. Imports build_options (the comptime gate)
//! and staging (Table.all) but NOTHING native, so it runs identically in all
//! three build variants (-Dstore default / true / false). It exercises:
//!   1. local_store default: null/absent => effective true; false => false.
//!   2. store_active composition: build_options.store_enabled AND local_store.
//!   3. ignore_unknown_fields: a config carrying store knobs (roll_max_bytes,
//!      storage_refresh_interval, local_store) parses under a store=false daemon
//!      without crashing - the forward-compat contract.
//!   4. local_store=false skips the per-cycle staging append + roll: a synthetic
//!      cycle driven with store_active=false invokes neither the append nor the
//!      roll spy (only meaningful in a store-capable build, so gated).
const std = @import("std");
const build_options = @import("build_options");
const staging = @import("staging");

// Minimal mirror of src/agent/main.zig's Config: ONLY the fields these tests
// assert on, plus a representative back-compat knob, so the parse + default
// logic is exercised without dragging the whole daemon in. ignore_unknown_fields
// makes any field the daemon-side Config has but this mirror omits a no-op.
const Config = struct {
    local_store: ?bool = null,
    roll_max_bytes: ?u64 = null,
    storage_refresh_interval: ?u64 = null,
};

/// The exact runtime resolution the daemon uses: null (absent) => true.
fn effectiveLocalStore(cfg: ?Config) bool {
    return if (cfg) |c| c.local_store orelse true else true;
}

/// store_active = comptime build gate AND runtime knob, exactly as main.zig.
fn storeActive(local_store: bool) bool {
    return build_options.store_enabled and local_store;
}

fn parseConfig(allocator: std.mem.Allocator, json: []const u8) !std.json.Parsed(Config) {
    return std.json.parseFromSlice(Config, allocator, json, .{
        .ignore_unknown_fields = true,
        .allocate = .alloc_always,
    });
}

test "local_store default: null/absent => effective true" {
    const ally = std.testing.allocator;
    // Empty config object: local_store absent.
    var parsed = try parseConfig(ally, "{}");
    defer parsed.deinit();
    try std.testing.expect(parsed.value.local_store == null);
    try std.testing.expectEqual(true, effectiveLocalStore(parsed.value));
    // No config at all => also true.
    try std.testing.expectEqual(true, effectiveLocalStore(null));
}

test "local_store=false => effective false" {
    const ally = std.testing.allocator;
    var parsed = try parseConfig(ally, "{\"local_store\": false}");
    defer parsed.deinit();
    try std.testing.expectEqual(@as(?bool, false), parsed.value.local_store);
    try std.testing.expectEqual(false, effectiveLocalStore(parsed.value));
}

test "local_store=true => effective true" {
    const ally = std.testing.allocator;
    var parsed = try parseConfig(ally, "{\"local_store\": true}");
    defer parsed.deinit();
    try std.testing.expectEqual(true, effectiveLocalStore(parsed.value));
}

test "store_active composes the build gate with the runtime knob" {
    // local_store=false ALWAYS yields inactive, regardless of build.
    try std.testing.expectEqual(false, storeActive(false));
    // local_store=true yields active iff the build is store-capable.
    try std.testing.expectEqual(build_options.store_enabled, storeActive(true));
}

test "config carrying store knobs parses under ignore_unknown_fields" {
    const ally = std.testing.allocator;
    // A daemon (store=false or store=true) reading a config that sets local_store,
    // roll_max_bytes, storage_refresh_interval AND a field this mirror does not
    // know must parse cleanly and ignore the unknown one - forward/back compat.
    const json =
        \\{"local_store": false, "roll_max_bytes": 1048576,
        \\ "storage_refresh_interval": 30, "some_future_knob": "x"}
    ;
    var parsed = try parseConfig(ally, json);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(?bool, false), parsed.value.local_store);
    try std.testing.expectEqual(@as(?u64, 1048576), parsed.value.roll_max_bytes);
    try std.testing.expectEqual(@as(?u64, 30), parsed.value.storage_refresh_interval);
}

// ── Synthetic per-cycle write gate ──
// Models the daemon's per-cycle decision: only when store_active do we append to
// staging and evaluate the roll trigger. Spies record whether each path ran. This
// is the unit under "local_store=false skips staging append+roll".
const CycleSpy = struct {
    appended: bool = false,
    rolled: bool = false,

    fn runCycle(self: *CycleSpy, store_active: bool) void {
        if (store_active) {
            self.appended = true; // stands in for stg.append*
            // roll trigger is evaluated only inside the same store_active gate
            for (staging.Table.all) |_| {
                self.rolled = true; // stands in for roll_mod.rollTable
            }
        }
    }
};

test "local_store=false skips staging append + roll" {
    // Only meaningful where the write path exists (store-capable build); in the
    // minimal build store_active is always false so the assertion below is the
    // same, but the roll/append it guards is compiled out of the real daemon.
    var spy = CycleSpy{};
    spy.runCycle(storeActive(false)); // local_store=false
    try std.testing.expectEqual(false, spy.appended);
    try std.testing.expectEqual(false, spy.rolled);
}

test "local_store=true drives staging append + roll in a store build" {
    var spy = CycleSpy{};
    spy.runCycle(storeActive(true)); // local_store=true
    // Active exactly when the build is store-capable.
    try std.testing.expectEqual(build_options.store_enabled, spy.appended);
    try std.testing.expectEqual(build_options.store_enabled, spy.rolled);
}
