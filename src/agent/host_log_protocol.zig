//! Closed, observation-only hosted log query contract. No execution strings.
const std = @import("std");
pub const version = 1;
pub const minimum_daemon_version = "0.0.2";
pub const ErrorCode = enum { unsupported_filter, store_unavailable, query_timeout, redaction_failed, query_failed };
pub const Filters = struct {
    since: []const u8,
    until: []const u8,
    unit: ?[]const u8 = null,
    identifier: ?[]const u8 = null,
    systemd_unit: ?[]const u8 = null,
    service: ?[]const u8 = null,
    max_priority: ?u8 = null,
    trace_id: ?[]const u8 = null,
    max_rows: u16 = 100,
    max_bytes: u32 = 131072,

    pub fn validate(self: Filters) !void {
        const lo = try timestamp(self.since);
        const hi = try timestamp(self.until);
        if (hi <= lo or hi - lo > 86400_000000) return error.InvalidFilters;
        inline for (.{ self.unit, self.identifier, self.systemd_unit, self.service }) |s| {
            if (s) |v| if (!validString(v, 255)) return error.InvalidFilters;
        }
        if (self.max_priority) |p| if (p > 7) return error.InvalidFilters;
        if (self.trace_id) |t| if (!validTrace(t)) return error.InvalidFilters;
        if (self.max_rows == 0 or self.max_rows > 200 or self.max_bytes < 1024 or self.max_bytes > 131072) return error.InvalidFilters;
    }
};
pub const Claim = struct {
    id: []const u8,
    protocol_version: u8,
    claim_token: []const u8,
    expires_at: []const u8,
    filters: Filters,

    pub fn deadline(self: Claim) !i64 {
        if (self.protocol_version != version or !uuid(self.id) or !uuid(self.claim_token)) return error.InvalidClaim;
        try self.filters.validate();
        return timestamp(self.expires_at);
    }
};
pub const ClaimResponse = struct { protocol_version: u8, request: ?Claim, poll_after_seconds: u8 };
pub const Row = struct {
    timestamp: []const u8,
    source: []const u8,
    unit: ?[]const u8,
    identifier: ?[]const u8,
    systemd_unit: ?[]const u8,
    priority: u8,
    message: []const u8,
    trace_id: ?[]const u8 = null,
};
pub const Coverage = struct {
    snapshot_at: []const u8,
    oldest_available_at: ?[]const u8 = null,
    newest_available_at: ?[]const u8 = null,
    complete: bool = false,
    gaps: []const []const u8 = &.{"unknown"},
};
pub const Result = struct { rows: []const Row, truncated: bool, coverage: Coverage };

pub fn validString(s: []const u8, max: usize) bool {
    return s.len > 0 and s.len <= max and std.mem.indexOfScalar(u8, s, 0) == null and std.unicode.utf8ValidateSlice(s);
}
pub fn validTrace(s: []const u8) bool {
    if (s.len != 32) return false;
    var nonzero = false;
    for (s) |ch| {
        if (!std.ascii.isDigit(ch) and !(ch >= 'a' and ch <= 'f')) return false;
        nonzero = nonzero or ch != '0';
    }
    return nonzero;
}
fn uuid(s: []const u8) bool {
    if (s.len != 36) return false;
    for (s, 0..) |ch, i| {
        if (i == 8 or i == 13 or i == 18 or i == 23) {
            if (ch != '-') return false;
        } else if (!std.ascii.isHex(ch)) return false;
    }
    return true;
}
fn digits(s: []const u8) !u32 {
    for (s) |ch| if (!std.ascii.isDigit(ch)) return error.InvalidTimestamp;
    return std.fmt.parseInt(u32, s, 10) catch error.InvalidTimestamp;
}
/// UTC only, microsecond precision; never silently round a filter boundary.
pub fn timestamp(s: []const u8) !i64 {
    if (s.len < 20 or s.len > 27 or s[4] != '-' or s[7] != '-' or s[10] != 'T' or s[13] != ':' or s[16] != ':' or s[s.len - 1] != 'Z') return error.InvalidTimestamp;
    const y = try digits(s[0..4]);
    const m = try digits(s[5..7]);
    const d = try digits(s[8..10]);
    const h = try digits(s[11..13]);
    const min = try digits(s[14..16]);
    const sec = try digits(s[17..19]);
    if (y < 1970 or y > 9999 or m == 0 or m > 12 or d == 0 or h > 23 or min > 59 or sec > 59) return error.InvalidTimestamp;
    const year: std.time.epoch.Year = @intCast(y);
    if (d > std.time.epoch.getDaysInMonth(year, @enumFromInt(m))) return error.InvalidTimestamp;
    var days: i64 = 0;
    var yr: std.time.epoch.Year = 1970;
    while (yr < year) : (yr += 1) days += std.time.epoch.getDaysInYear(yr);
    var month: u4 = 1;
    while (month < m) : (month += 1) days += std.time.epoch.getDaysInMonth(year, @enumFromInt(month));
    days += d - 1;
    var micros: u32 = 0;
    if (s.len != 20) {
        if (s[19] != '.' or s.len < 22) return error.InvalidTimestamp;
        micros = try digits(s[20 .. s.len - 1]);
        var n = s.len - 21;
        while (n < 6) : (n += 1) micros *= 10;
    }
    return (days * 86400 + h * 3600 + min * 60 + sec) * 1000000 + micros;
}
pub fn formatTimestamp(a: std.mem.Allocator, seconds: i64) ![]const u8 {
    if (seconds < 0) return error.InvalidTimestamp;
    const es = std.time.epoch.EpochSeconds{ .secs = @intCast(seconds) };
    const day = es.getEpochDay();
    const yd = day.calculateYearDay();
    const md = yd.calculateMonthDay();
    const ds = es.getDaySeconds();
    return std.fmt.allocPrint(a, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{ yd.year, @intFromEnum(md.month), md.day_index + 1, ds.getHoursIntoDay(), ds.getMinutesIntoHour(), ds.getSecondsIntoMinute() });
}
/// Zig's typed parser accepts numeric strings. Check JSON tags first, then let
/// its closed struct parser reject every unknown key at every nesting level.
pub fn parseClaim(a: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(ClaimResponse) {
    if (bytes.len > 8192) return error.InvalidClaim;
    const v = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer v.deinit();
    try strictTypes(v.value);
    const p = try std.json.parseFromSlice(ClaimResponse, a, bytes, .{ .allocate = .alloc_always });
    errdefer p.deinit();
    if (p.value.protocol_version != version or p.value.poll_after_seconds != 10) return error.InvalidClaim;
    if (p.value.request) |r| _ = try r.deadline();
    return p;
}
fn strictTypes(v: std.json.Value) !void {
    if (v != .object) return error.InvalidClaim;
    var it = v.object.iterator();
    while (it.next()) |e| {
        const k = e.key_ptr.*;
        const val = e.value_ptr.*;
        if (std.mem.eql(u8, k, "request") and val == .null) continue;
        if (std.mem.eql(u8, k, "filters") or std.mem.eql(u8, k, "request")) {
            try strictTypes(val);
        } else if (std.mem.eql(u8, k, "protocol_version") or std.mem.eql(u8, k, "poll_after_seconds") or std.mem.startsWith(u8, k, "max_")) {
            if (val != .integer) return error.InvalidClaim;
        } else if (val != .string) return error.InvalidClaim;
    }
}

test "UTC boundaries, calendar and exact caps" {
    const t = std.testing;
    try t.expectEqual(@as(i64, 86400_000000), try timestamp("1970-01-02T00:00:00Z"));
    try t.expectError(error.InvalidTimestamp, timestamp("2026-02-29T00:00:00Z"));
    try t.expectError(error.InvalidTimestamp, timestamp("2026-01-01T00:00:00+00:00"));
    var f = Filters{ .since = "2026-01-01T00:00:00Z", .until = "2026-01-02T00:00:00Z" };
    try f.validate();
    f.until = "2026-01-02T00:00:00.000001Z";
    try t.expectError(error.InvalidFilters, f.validate());
    try t.expect(!validTrace("00000000000000000000000000000000"));
    try t.expect(validTrace("1234567890abcdef1234567890abcdef"));
}
test "closed JSON rejects coercion and unknown fields" {
    const a = std.testing.allocator;
    const ok = try parseClaim(a, "{\"protocol_version\":1,\"request\":null,\"poll_after_seconds\":10}");
    defer ok.deinit();
    try std.testing.expectError(error.InvalidClaim, parseClaim(a, "{\"protocol_version\":\"1\",\"request\":null,\"poll_after_seconds\":10}"));
    try std.testing.expectError(error.UnknownField, parseClaim(a, "{\"protocol_version\":1,\"request\":null,\"poll_after_seconds\":10,\"sql\":\"SELECT 1\"}"));
}
