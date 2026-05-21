//! Push-time log rules.
//!
//! Rules decide which log entries are eligible to upload to a hosted Sermon
//! server. They gate the push payload only - every entry is still written to
//! local DuckDB at full fidelity (see agent/main.zig). The worst a rule can do
//! is make the hosted view too quiet; the firehose is always recoverable
//! locally.
//!
//! A rule is a list of conditions AND-ed together; multiple rules give OR.
//! Evaluation is first-match-wins, and an entry matching no rule defaults to
//! `.keep`, so an empty rule list preserves the daemon's current behavior.
//!
//! Note: a severity floor (errors always push) is intentionally NOT here -
//! it is a push-time policy applied by the caller in agent/push.zig. This
//! module is pure rule evaluation.

const std = @import("std");
const logs = @import("logs");

/// What to do with a log entry that matches a rule.
pub const Action = enum { keep, drop, sample };

/// Comparison operator for a single condition. String-only in V1; `eq` and
/// `contains` use `std.mem`, so matching is linear-time and cannot hang the
/// single-threaded collection loop the way a backtracking regex could.
pub const Op = enum { eq, contains, not_contains };

/// Which field of a log entry a condition tests.
pub const Field = enum { source, identifier, systemd_unit, message };

/// One condition: `field op value`. All conditions in a rule AND together.
pub const Condition = struct {
    field: Field,
    op: Op,
    value: []const u8,
};

/// A single rule, parsed straight from the `log_rules` array in the rules
/// file. An empty `match` list matches every entry (the catch-all).
pub const LogRule = struct {
    name: ?[]const u8 = null,
    description: ?[]const u8 = null,
    match: []const Condition = &.{},
    action: Action = .keep,
    /// For `.sample`: keep 1 in `keep_one_in` matching entries. Ignored
    /// otherwise. A value of 0 is treated as 1 by the sampler.
    keep_one_in: u32 = 1,
};

/// The on-disk rules file: `{ "version": 1, "log_rules": [...] }`.
pub const RuleFile = struct {
    version: u32 = 1,
    log_rules: []const LogRule = &.{},
};

/// Outcome of evaluating the rule list against one entry. `keep_one_in` is
/// carried alongside the action so the caller can apply `.sample` without
/// re-reading the matched rule.
pub const Decision = struct {
    action: Action = .keep,
    keep_one_in: u32 = 1,
};

fn fieldValue(field: Field, entry: logs.LogEntry) ?[]const u8 {
    return switch (field) {
        .source => entry.source,
        .identifier => entry.identifier,
        .systemd_unit => entry.systemd_unit,
        .message => entry.message,
    };
}

/// True when `cond` holds for `entry`. A condition on an absent (null) field
/// is false for `eq`/`contains` but true for `not_contains` - an absent field
/// genuinely does not contain the value.
pub fn conditionMatches(c: Condition, entry: logs.LogEntry) bool {
    const maybe = fieldValue(c.field, entry);
    return switch (c.op) {
        .eq => if (maybe) |got| std.mem.eql(u8, got, c.value) else false,
        .contains => if (maybe) |got| std.mem.indexOf(u8, got, c.value) != null else false,
        .not_contains => if (maybe) |got| std.mem.indexOf(u8, got, c.value) == null else true,
    };
}

/// True when every condition in `rule` holds (AND). An empty `match` list is
/// vacuously true - it matches everything.
pub fn matches(rule: LogRule, entry: logs.LogEntry) bool {
    for (rule.match) |c| {
        if (!conditionMatches(c, entry)) return false;
    }
    return true;
}

/// Index of the first rule matching `entry`, or null if none match.
pub fn matchIndex(rules: []const LogRule, entry: logs.LogEntry) ?usize {
    for (rules, 0..) |rule, i| {
        if (matches(rule, entry)) return i;
    }
    return null;
}

/// First matching rule wins. No match defaults to `.keep`. Pure - does not
/// advance sample counters; see `RuleSet.eligible` for the stateful path.
pub fn decide(rules: []const LogRule, entry: logs.LogEntry) Decision {
    const i = matchIndex(rules, entry) orelse return .{};
    return .{ .action = rules[i].action, .keep_one_in = rules[i].keep_one_in };
}

/// A rule list paired with the mutable per-rule counters that `.sample` rules
/// need. `sample_counts` is indexed parallel to `rules` and persists across
/// push cycles, so "keep 1 in N" thins the whole stream rather than each
/// collection cycle in isolation.
pub const RuleSet = struct {
    rules: []const LogRule = &.{},
    sample_counts: []u64 = &.{},

    /// Whether `entry` should be uploaded, advancing sample counters as a
    /// side effect. A `.sample` rule keeps every Nth matching entry; `.keep`
    /// and `.drop` are returned directly; an entry matching no rule defaults
    /// to keep.
    pub fn eligible(self: RuleSet, entry: logs.LogEntry) bool {
        const i = matchIndex(self.rules, entry) orelse return true;
        return switch (self.rules[i].action) {
            .keep => true,
            .drop => false,
            .sample => blk: {
                if (i >= self.sample_counts.len) break :blk true; // no counter slot
                const n: u64 = @max(self.rules[i].keep_one_in, 1);
                self.sample_counts[i] += 1;
                break :blk self.sample_counts[i] % n == 0;
            },
        };
    }
};

/// The result of rebuilding a combined rule set from local and server rules.
/// Caller frees `rules` and `counts` (e.g. when the next refresh replaces
/// them). The local prefix of `counts` is preserved from `prev_counts`; the
/// server suffix starts at zero so newly-pushed sample rules begin fresh.
pub const CombinedRules = struct {
    rules: []LogRule,
    counts: []u64,
};

/// Concatenate `local` rules and `server` rules into a freshly-allocated
/// combined slice, with parallel counters. Local first means an operator's
/// hand-edited rules are evaluated before server-pushed ones - "operator
/// override the control plane."
pub fn combineRules(
    allocator: std.mem.Allocator,
    local: []const LogRule,
    server: []const LogRule,
    prev_counts: []const u64,
) !CombinedRules {
    const total = local.len + server.len;
    const rules = try allocator.alloc(LogRule, total);
    errdefer allocator.free(rules);
    const counts = try allocator.alloc(u64, total);
    errdefer allocator.free(counts);

    @memcpy(rules[0..local.len], local);
    @memcpy(rules[local.len..], server);

    const preserve_n = @min(local.len, prev_counts.len);
    @memcpy(counts[0..preserve_n], prev_counts[0..preserve_n]);
    @memset(counts[preserve_n..], 0);

    return .{ .rules = rules, .counts = counts };
}

// ── Tests ──

fn testEntry() logs.LogEntry {
    return .{
        .timestamp = 0,
        .source = "systemd",
        .unit = "nginx",
        .identifier = "nginx",
        .systemd_unit = "nginx.service",
        .priority = 6,
        .message = "GET /health 200",
        .pid = null,
    };
}

fn cond(field: Field, op: Op, value: []const u8) Condition {
    return .{ .field = field, .op = op, .value = value };
}

test "empty rule list keeps everything" {
    try std.testing.expectEqual(Action.keep, decide(&.{}, testEntry()).action);
}

test "empty match list is a catch-all" {
    const rules = [_]LogRule{
        .{ .match = &.{}, .action = .drop },
    };
    try std.testing.expectEqual(Action.drop, decide(&rules, testEntry()).action);
}

test "single eq condition matches" {
    const rules = [_]LogRule{
        .{ .match = &.{cond(.systemd_unit, .eq, "nginx.service")}, .action = .drop },
    };
    try std.testing.expectEqual(Action.drop, decide(&rules, testEntry()).action);
}

test "single eq condition that differs does not match" {
    const rules = [_]LogRule{
        .{ .match = &.{cond(.systemd_unit, .eq, "postgres.service")}, .action = .drop },
    };
    try std.testing.expectEqual(Action.keep, decide(&rules, testEntry()).action);
}

test "contains matches a substring of the body" {
    const rules = [_]LogRule{
        .{ .match = &.{cond(.message, .contains, "/health")}, .action = .drop },
    };
    try std.testing.expectEqual(Action.drop, decide(&rules, testEntry()).action);
}

test "contains is case-sensitive" {
    const rules = [_]LogRule{
        .{ .match = &.{cond(.message, .contains, "get")}, .action = .drop },
    };
    // Body has "GET", not "get" - no match.
    try std.testing.expectEqual(Action.keep, decide(&rules, testEntry()).action);
}

test "not_contains is true when the substring is absent" {
    const rules = [_]LogRule{
        .{ .match = &.{cond(.message, .not_contains, "error")}, .action = .drop },
    };
    try std.testing.expectEqual(Action.drop, decide(&rules, testEntry()).action);
}

test "not_contains is false when the substring is present" {
    const rules = [_]LogRule{
        .{ .match = &.{cond(.message, .not_contains, "/health")}, .action = .drop },
    };
    try std.testing.expectEqual(Action.keep, decide(&rules, testEntry()).action);
}

test "eq on an absent field never matches" {
    var entry = testEntry();
    entry.identifier = null;
    const rules = [_]LogRule{
        .{ .match = &.{cond(.identifier, .eq, "nginx")}, .action = .drop },
    };
    try std.testing.expectEqual(Action.keep, decide(&rules, entry).action);
}

test "not_contains on an absent field is true" {
    // An absent identifier genuinely does not contain anything.
    var entry = testEntry();
    entry.identifier = null;
    const rules = [_]LogRule{
        .{ .match = &.{cond(.identifier, .not_contains, "nginx")}, .action = .drop },
    };
    try std.testing.expectEqual(Action.drop, decide(&rules, entry).action);
}

test "all conditions in a rule must hold (AND)" {
    // Unit matches but the message condition does not - the rule should not fire.
    const rules = [_]LogRule{
        .{
            .match = &.{
                cond(.systemd_unit, .eq, "nginx.service"),
                cond(.message, .contains, "panic"),
            },
            .action = .drop,
        },
    };
    try std.testing.expectEqual(Action.keep, decide(&rules, testEntry()).action);
}

test "two message conditions express contains-X-but-not-Y in one rule" {
    // Drop nginx lines that mention "GET" but are not health checks.
    const rules = [_]LogRule{
        .{
            .match = &.{
                cond(.message, .contains, "GET"),
                cond(.message, .not_contains, "/health"),
            },
            .action = .drop,
        },
    };
    // The health-check line is exempt (not_contains "/health" fails).
    try std.testing.expectEqual(Action.keep, decide(&rules, testEntry()).action);

    var api_entry = testEntry();
    api_entry.message = "GET /api/users 200";
    try std.testing.expectEqual(Action.drop, decide(&rules, api_entry).action);
}

test "first matching rule wins - keep before drop is an allowlist" {
    // "Only the errors from nginx": keep error lines, drop the rest of nginx.
    const keep_errors_drop_rest = [_]LogRule{
        .{
            .match = &.{
                cond(.systemd_unit, .eq, "nginx.service"),
                cond(.message, .contains, "error"),
            },
            .action = .keep,
        },
        .{ .match = &.{cond(.systemd_unit, .eq, "nginx.service")}, .action = .drop },
    };

    // A health-check line is not an error, so it falls through to the drop.
    try std.testing.expectEqual(
        Action.drop,
        decide(&keep_errors_drop_rest, testEntry()).action,
    );

    // An actual error line is caught by the first rule and kept.
    var err_entry = testEntry();
    err_entry.message = "error: upstream timed out";
    try std.testing.expectEqual(
        Action.keep,
        decide(&keep_errors_drop_rest, err_entry).action,
    );
}

test "sample decision carries keep_one_in" {
    const rules = [_]LogRule{
        .{
            .match = &.{cond(.identifier, .eq, "nginx")},
            .action = .sample,
            .keep_one_in = 100,
        },
    };
    const decision = decide(&rules, testEntry());
    try std.testing.expectEqual(Action.sample, decision.action);
    try std.testing.expectEqual(@as(u32, 100), decision.keep_one_in);
}

test "matchIndex returns the first matching rule" {
    const rules = [_]LogRule{
        .{ .match = &.{cond(.systemd_unit, .eq, "other.service")}, .action = .drop },
        .{ .match = &.{cond(.identifier, .eq, "nginx")}, .action = .keep },
        .{ .match = &.{cond(.identifier, .eq, "nginx")}, .action = .drop },
    };
    try std.testing.expectEqual(@as(?usize, 1), matchIndex(&rules, testEntry()));
    try std.testing.expectEqual(@as(?usize, null), matchIndex(&.{}, testEntry()));
}

test "RuleSet.eligible keeps unmatched entries and applies keep/drop" {
    const rules = [_]LogRule{
        .{ .match = &.{cond(.systemd_unit, .eq, "nginx.service")}, .action = .drop },
    };
    var counts = [_]u64{0};
    const set = RuleSet{ .rules = &rules, .sample_counts = &counts };

    try std.testing.expect(!set.eligible(testEntry())); // matches the drop rule

    var other = testEntry();
    other.systemd_unit = "postgres.service";
    try std.testing.expect(set.eligible(other)); // matches nothing -> keep
}

test "RuleSet.eligible samples - keeps every Nth match" {
    const rules = [_]LogRule{
        .{ .match = &.{cond(.identifier, .eq, "nginx")}, .action = .sample, .keep_one_in = 3 },
    };
    var counts = [_]u64{0};
    const set = RuleSet{ .rules = &rules, .sample_counts = &counts };

    var kept: usize = 0;
    var i: usize = 0;
    while (i < 9) : (i += 1) {
        if (set.eligible(testEntry())) kept += 1;
    }
    // 9 matching entries at keep_one_in = 3 -> the 3rd, 6th, 9th survive.
    try std.testing.expectEqual(@as(usize, 3), kept);
}

test "RuleSet.eligible: a sample rule with default keep_one_in keeps all" {
    const rules = [_]LogRule{
        .{ .match = &.{cond(.identifier, .eq, "nginx")}, .action = .sample },
    };
    var counts = [_]u64{0};
    const set = RuleSet{ .rules = &rules, .sample_counts = &counts };

    try std.testing.expect(set.eligible(testEntry()));
    try std.testing.expect(set.eligible(testEntry()));
}

test "combineRules concatenates local then server and preserves local counts" {
    const allocator = std.testing.allocator;
    const local = [_]LogRule{
        .{ .match = &.{cond(.identifier, .eq, "nginx")}, .action = .sample, .keep_one_in = 10 },
    };
    const server = [_]LogRule{
        .{ .match = &.{cond(.systemd_unit, .eq, "cron.service")}, .action = .drop },
    };
    const prev_counts = [_]u64{42};

    const combined = try combineRules(allocator, &local, &server, &prev_counts);
    defer allocator.free(combined.rules);
    defer allocator.free(combined.counts);

    // Local first, then server.
    try std.testing.expectEqual(@as(usize, 2), combined.rules.len);
    try std.testing.expectEqual(Action.sample, combined.rules[0].action);
    try std.testing.expectEqual(Action.drop, combined.rules[1].action);
    // Local counter preserved, server counter starts fresh.
    try std.testing.expectEqual(@as(u64, 42), combined.counts[0]);
    try std.testing.expectEqual(@as(u64, 0), combined.counts[1]);
}

test "combineRules handles an empty local list" {
    const allocator = std.testing.allocator;
    const server = [_]LogRule{
        .{ .match = &.{}, .action = .drop },
    };
    const prev_counts: [0]u64 = .{};

    const combined = try combineRules(allocator, &.{}, &server, &prev_counts);
    defer allocator.free(combined.rules);
    defer allocator.free(combined.counts);

    try std.testing.expectEqual(@as(usize, 1), combined.rules.len);
    try std.testing.expectEqual(@as(u64, 0), combined.counts[0]);
}

test "combineRules handles an empty server list" {
    const allocator = std.testing.allocator;
    const local = [_]LogRule{
        .{ .match = &.{cond(.identifier, .eq, "nginx")}, .action = .keep },
    };
    const prev_counts = [_]u64{7};

    const combined = try combineRules(allocator, &local, &.{}, &prev_counts);
    defer allocator.free(combined.rules);
    defer allocator.free(combined.counts);

    try std.testing.expectEqual(@as(usize, 1), combined.rules.len);
    try std.testing.expectEqual(@as(u64, 7), combined.counts[0]);
}
