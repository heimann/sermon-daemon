//! OTLP/HTTP local collector (plan 26).
//!
//! This module is the daemon-side OTLP receiver. The thesis-pure on-host path
//! is: apps emit OTLP to a localhost daemon receiver at full fidelity, the
//! daemon keeps it on-host (queryable over SSH like the existing /proc +
//! journald data), and forwards a sampled copy to the hosted /v1/* endpoints.
//!
//! SCOPE OF THIS FILE (plan 26 phase 1 / scaffold):
//!   - OTLP/JSON envelope structs for the *logs* signal.
//!   - `mapLogRecord`: a PURE mapping from one decoded OTLP/JSON LogRecord to
//!     the daemon's canonical on-host log representation (`logs.LogEntry`),
//!     so received OTLP logs flow through the same local-write + forward
//!     machinery as journald logs.
//!
//! DELIBERATELY NOT HERE YET (see docs/plans/26-daemon-otlp-local-collector.md):
//!   - The `std.http.Server` listen/accept loop (phase 2). v1 is a
//!     single-threaded synchronous accept loop on its own thread, spawned only
//!     when `receiver_enabled = true`. It is a documented TODO stub below; the
//!     daemon default (`receiver_enabled = false`) means it is never reached.
//!   - On-host persistence into the parquet hot tier (phase 3).
//!   - Metrics + traces (phases 4-5), OTLP/protobuf (phase 6).

const std = @import("std");
const logs_mod = @import("logs");

const Allocator = std.mem.Allocator;

pub const default_receiver_port: u16 = 4318; // OTLP/HTTP standard port.

/// OTLP/JSON logs request envelope: `resourceLogs[].scopeLogs[].logRecords[]`.
/// These mirror the OTLP/JSON encoding of the OpenTelemetry logs data model.
/// Fields the daemon does not consume yet are intentionally omitted; the JSON
/// parser is configured with `ignore_unknown_fields = true` so a fuller payload
/// from a real SDK still parses.
pub const LogsData = struct {
    resourceLogs: []const ResourceLogs = &.{},
};

pub const ResourceLogs = struct {
    resource: ?Resource = null,
    scopeLogs: []const ScopeLogs = &.{},
};

pub const Resource = struct {
    attributes: []const KeyValue = &.{},
};

pub const ScopeLogs = struct {
    logRecords: []const LogRecord = &.{},
};

/// One OTLP log record. In OTLP/JSON, `timeUnixNano` is a *string* (it can
/// exceed JS-safe integers), `body` is an AnyValue (we consume `stringValue`),
/// and severity is `severityNumber` (1-24) plus optional `severityText`.
pub const LogRecord = struct {
    timeUnixNano: ?[]const u8 = null,
    observedTimeUnixNano: ?[]const u8 = null,
    severityNumber: ?i64 = null,
    severityText: ?[]const u8 = null,
    body: ?AnyValue = null,
    attributes: []const KeyValue = &.{},
};

pub const KeyValue = struct {
    key: []const u8,
    value: ?AnyValue = null,
};

/// OTLP AnyValue. We only consume `stringValue` in v1; other variants parse and
/// are ignored. `ignore_unknown_fields` keeps richer values from breaking parse.
pub const AnyValue = struct {
    stringValue: ?[]const u8 = null,
};

/// Map an OTLP `severityNumber` (1-24) to a syslog priority (0=emerg..7=debug),
/// matching `logs.LogEntry.priority`. This is the inverse-direction mapping the
/// push-side severity floor (`always_push_priority`) relies on, so it must land
/// real errors at low (severe) syslog numbers.
///
/// OTLP severity bands: 1-4 TRACE, 5-8 DEBUG, 9-12 INFO, 13-16 WARN,
/// 17-20 ERROR, 21-24 FATAL. A missing/0 severity is treated as INFO(6).
pub fn syslogPriorityFromSeverity(severity_number: ?i64) u8 {
    const n = severity_number orelse return 6;
    if (n >= 21) return 2; // FATAL -> crit
    if (n >= 17) return 3; // ERROR -> err
    if (n >= 13) return 4; // WARN  -> warning
    if (n >= 9) return 6; // INFO  -> info
    if (n >= 5) return 7; // DEBUG -> debug
    if (n >= 1) return 7; // TRACE -> debug (no finer syslog level)
    return 6; // unspecified -> info
}

/// Convert an OTLP `timeUnixNano` string (nanoseconds since epoch) to the
/// daemon's Unix *seconds* timestamp. A missing/empty/unparseable value falls
/// back to `now` so a record is never dropped purely for a bad timestamp.
pub fn unixSecondsFromNano(time_unix_nano: ?[]const u8, now: i64) i64 {
    const s = time_unix_nano orelse return now;
    if (s.len == 0) return now;
    const nanos = std.fmt.parseInt(i128, s, 10) catch return now;
    return @intCast(@divTrunc(nanos, std.time.ns_per_s));
}

/// PURE mapping: one decoded OTLP/JSON `LogRecord` -> an owned `logs.LogEntry`.
///
/// `service_name` is the resource attribute `service.name` for the owning
/// `ResourceLogs` (or null); it becomes the entry's identifier/systemd_unit so
/// received OTLP logs attribute to a service the same way journald logs
/// attribute to a unit. `now` is the fallback timestamp.
///
/// The returned `LogEntry` OWNS its strings (duped from `allocator`); the caller
/// frees them via `LogEntry.deinit(allocator)`, exactly like a journald entry.
/// On allocation failure any partially-duped strings are freed before erroring.
pub fn mapLogRecord(
    allocator: Allocator,
    record: LogRecord,
    service_name: ?[]const u8,
    now: i64,
) !logs_mod.LogEntry {
    const message_src: []const u8 = blk: {
        const body = record.body orelse break :blk "";
        break :blk body.stringValue orelse "";
    };

    const source = try allocator.dupe(u8, "otlp");
    errdefer allocator.free(source);

    const message = try allocator.dupe(u8, message_src);
    errdefer allocator.free(message);

    // identifier and systemd_unit both carry the service name (when present) so
    // OTLP records group and filter like journald entries. `unit` is the
    // back-compat grouping key: identifier if present, else systemd_unit.
    var identifier: ?[]const u8 = null;
    errdefer if (identifier) |v| allocator.free(v);
    var systemd_unit: ?[]const u8 = null;
    errdefer if (systemd_unit) |v| allocator.free(v);
    var unit: ?[]const u8 = null;
    errdefer if (unit) |v| allocator.free(v);

    if (service_name) |svc| {
        if (svc.len > 0) {
            identifier = try allocator.dupe(u8, svc);
            systemd_unit = try allocator.dupe(u8, svc);
            unit = try allocator.dupe(u8, svc);
        }
    }

    return .{
        .timestamp = unixSecondsFromNano(record.timeUnixNano, now),
        .source = source,
        .unit = unit,
        .identifier = identifier,
        .systemd_unit = systemd_unit,
        .priority = syslogPriorityFromSeverity(record.severityNumber),
        .message = message,
        .pid = null,
    };
}

/// Look up the `service.name` resource attribute in a `ResourceLogs`, if any.
/// Returns a borrowed slice into the parsed JSON (valid for the parse lifetime);
/// `mapLogRecord` dupes it.
pub fn serviceName(resource_logs: ResourceLogs) ?[]const u8 {
    const resource = resource_logs.resource orelse return null;
    for (resource.attributes) |kv| {
        if (std.mem.eql(u8, kv.key, "service.name")) {
            const v = kv.value orelse return null;
            return v.stringValue;
        }
    }
    return null;
}

// ── TODO (plan 26 phase 2): std.http.Server listen/accept loop ──
//
// A single-threaded SYNCHRONOUS accept loop on 127.0.0.1:receiver_port, run on
// its own std.Thread and spawned ONLY when receiver_enabled = true. It accepts
// one connection at a time, reads a `POST /v1/logs` OTLP/JSON body (bounded
// size), parses it into `LogsData`, maps each record via `mapLogRecord`, and
// hands the results to the forward path (push.zig-shaped client, Bearer
// otlp_token) and later the on-host staging writer.
//
// CONCURRENCY TRADEOFF (intentional for v1): one synchronous accept loop
// serializes producers; a hung client or slow forward blocks others. Acceptable
// because same-host SDKs batch/retry and forwarding will be decoupled from
// accept. See docs/plans/26-daemon-otlp-local-collector.md "Architecture".
//
// Left as a stub: the daemon default `receiver_enabled = false` means this code
// path is never reached today, so the scaffold ships without a listening socket.
pub fn runReceiverStub() void {
    // Intentionally empty. Wiring lands in phase 2.
}

test "syslogPriorityFromSeverity maps OTLP bands to syslog priorities" {
    try std.testing.expectEqual(@as(u8, 2), syslogPriorityFromSeverity(21)); // FATAL
    try std.testing.expectEqual(@as(u8, 3), syslogPriorityFromSeverity(17)); // ERROR
    try std.testing.expectEqual(@as(u8, 4), syslogPriorityFromSeverity(13)); // WARN
    try std.testing.expectEqual(@as(u8, 6), syslogPriorityFromSeverity(9)); // INFO
    try std.testing.expectEqual(@as(u8, 7), syslogPriorityFromSeverity(5)); // DEBUG
    try std.testing.expectEqual(@as(u8, 6), syslogPriorityFromSeverity(null)); // unset -> info
    try std.testing.expectEqual(@as(u8, 6), syslogPriorityFromSeverity(0)); // unspecified -> info
}

test "unixSecondsFromNano converts nanos to seconds and falls back to now" {
    try std.testing.expectEqual(@as(i64, 1_739_443_200), unixSecondsFromNano("1739443200000000000", 0));
    try std.testing.expectEqual(@as(i64, 42), unixSecondsFromNano(null, 42));
    try std.testing.expectEqual(@as(i64, 42), unixSecondsFromNano("", 42));
    try std.testing.expectEqual(@as(i64, 42), unixSecondsFromNano("not-a-number", 42));
}

test "mapLogRecord maps an OTLP log record to an owned LogEntry" {
    const allocator = std.testing.allocator;

    const record = LogRecord{
        .timeUnixNano = "1739443200000000000",
        .severityNumber = 17, // ERROR
        .severityText = "ERROR",
        .body = .{ .stringValue = "upstream connection refused" },
    };

    var entry = try mapLogRecord(allocator, record, "checkout-api", 0);
    defer entry.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 1_739_443_200), entry.timestamp);
    try std.testing.expectEqualStrings("otlp", entry.source);
    try std.testing.expectEqualStrings("upstream connection refused", entry.message);
    try std.testing.expectEqual(@as(u8, 3), entry.priority); // ERROR -> syslog err
    try std.testing.expectEqualStrings("checkout-api", entry.identifier.?);
    try std.testing.expectEqualStrings("checkout-api", entry.systemd_unit.?);
    try std.testing.expectEqualStrings("checkout-api", entry.unit.?);
    try std.testing.expect(entry.pid == null);
}

test "mapLogRecord tolerates missing body and missing service name" {
    const allocator = std.testing.allocator;

    const record = LogRecord{}; // everything defaulted/null

    var entry = try mapLogRecord(allocator, record, null, 99);
    defer entry.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 99), entry.timestamp); // fell back to now
    try std.testing.expectEqualStrings("", entry.message); // no body -> empty
    try std.testing.expectEqual(@as(u8, 6), entry.priority); // unset severity -> info
    try std.testing.expect(entry.identifier == null);
    try std.testing.expect(entry.systemd_unit == null);
    try std.testing.expect(entry.unit == null);
}

test "parse a full OTLP/JSON logs envelope and map each record" {
    const allocator = std.testing.allocator;
    const body =
        \\{
        \\  "resourceLogs": [{
        \\    "resource": { "attributes": [
        \\      { "key": "service.name", "value": { "stringValue": "checkout-api" } }
        \\    ]},
        \\    "scopeLogs": [{
        \\      "logRecords": [
        \\        { "timeUnixNano": "1739443200000000000", "severityNumber": 9,
        \\          "body": { "stringValue": "request handled" } },
        \\        { "timeUnixNano": "1739443201000000000", "severityNumber": 17,
        \\          "body": { "stringValue": "request failed" } }
        \\      ]
        \\    }]
        \\  }]
        \\}
    ;

    const parsed = try std.json.parseFromSlice(LogsData, allocator, body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    try std.testing.expectEqual(@as(usize, 1), parsed.value.resourceLogs.len);
    const rl = parsed.value.resourceLogs[0];
    try std.testing.expectEqualStrings("checkout-api", serviceName(rl).?);

    const records = rl.scopeLogs[0].logRecords;
    try std.testing.expectEqual(@as(usize, 2), records.len);

    var first = try mapLogRecord(allocator, records[0], serviceName(rl), 0);
    defer first.deinit(allocator);
    try std.testing.expectEqualStrings("request handled", first.message);
    try std.testing.expectEqual(@as(u8, 6), first.priority); // INFO
    try std.testing.expectEqualStrings("checkout-api", first.identifier.?);

    var second = try mapLogRecord(allocator, records[1], serviceName(rl), 0);
    defer second.deinit(allocator);
    try std.testing.expectEqualStrings("request failed", second.message);
    try std.testing.expectEqual(@as(u8, 3), second.priority); // ERROR -> err
}
