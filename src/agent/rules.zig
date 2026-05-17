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

/// First matching rule wins. No match defaults to `.keep`.
pub fn decide(rules: []const LogRule, entry: logs.LogEntry) Decision {
    for (rules) |rule| {
        if (matches(rule, entry)) {
            return .{ .action = rule.action, .keep_one_in = rule.keep_one_in };
        }
    }
    return .{};
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
