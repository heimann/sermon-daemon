//! One outbound control worker; queries run in a fixed child mode so a blocked
//! lock, filesystem read or DuckDB setup is covered by the same hard deadline.
const std = @import("std");
const p = @import("host_log_protocol");
const pq = @import("parquet_query");
const redact = @import("redact");
const logs = @import("logs");
const outbox = @import("durable_outbox");
const http = @import("control_http.zig");
const build_options = @import("build_options");
const c = @cImport({
    @cInclude("sys/time.h");
});
pub const httpChildMain = http.childMain;
pub const Worker = struct {
    root: []const u8,
    origin: []const u8,
    key: []const u8,
    stop: std.atomic.Value(bool) = .init(false),
    thread: ?std.Thread = null,
    rate_limit_until: i64 = 0,
    last_request_at: i64 = 0,

    pub fn start(self: *Worker) !void {
        self.thread = try std.Thread.spawn(.{}, run, .{self});
    }
    pub fn deinit(self: *Worker) void {
        self.stop.store(true, .release);
        if (self.thread) |t| t.join();
    }
    fn run(self: *Worker) void {
        var box = outbox.Outbox.open(self.root) catch return;
        defer box.close();
        // Prevent two daemon instances executing or delivering the same claim.
        const lock = box.dir.createFile("worker.lock", .{ .truncate = false, .mode = 0o600 }) catch return;
        defer lock.close();
        std.posix.flock(lock.handle, std.posix.LOCK.EX | std.posix.LOCK.NB) catch return;
        var next_control: i64 = 0;
        var next_upload: i64 = 0;
        var next_telemetry: i64 = 0;
        var backoff: u32 = 2;
        var upload_backoff: u32 = 2;
        var telemetry_backoff: u32 = 2;
        var suspended = false;
        while (!self.stop.load(.acquire)) {
            var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer arena.deinit();
            const a = arena.allocator();
            const now = std.time.milliTimestamp();
            if (!suspended and now >= self.rate_limit_until and now >= next_control and compatibleVersion()) {
                const delay = self.control(a, &box, false) catch blk: {
                    const delay = backoff;
                    backoff = @min(backoff * 2, 60);
                    break :blk delay;
                };
                if (delay == 0) suspended = true;
                if (delay == 10) backoff = 2;
                next_control = std.time.milliTimestamp() + jitter(delay);
            }
            if (!suspended and now >= self.rate_limit_until and now >= next_upload) {
                const delay = self.upload(a, &box) catch blk: {
                    std.log.warn("log upload unacknowledged: durable records retained for retry", .{});
                    const delay = upload_backoff;
                    upload_backoff = @min(upload_backoff * 2, 60);
                    break :blk delay;
                };
                if (delay == 10) upload_backoff = 2;
                if (delay == 0) suspended = true;
                next_upload = std.time.milliTimestamp() + jitter(delay);
            }
            if (!suspended and now >= self.rate_limit_until and now >= next_telemetry) {
                const delay = self.telemetry(a, &box) catch blk: {
                    const delay = telemetry_backoff;
                    telemetry_backoff = @min(telemetry_backoff * 2, 60);
                    break :blk delay;
                };
                if (delay == 10) telemetry_backoff = 2;
                if (delay == 0) suspended = true;
                next_telemetry = std.time.milliTimestamp() + jitter(delay);
            }
            std.Thread.sleep(100 * std.time.ns_per_ms);
        }
    }
    fn control(self: *Worker, a: std.mem.Allocator, box: *outbox.Outbox, fresh: bool) !u32 {
        if (try box.read(a, "claim.json", 8192)) |saved| {
            const parsed = try p.parseClaim(a, saved);
            const claim = parsed.value.request orelse return error.InvalidClaim;
            const expiry = try claim.deadline();
            const now = std.time.microTimestamp();
            if (expiry <= now + 5_000000 or expiry - now > 60_000000) {
                try clearClaim(box);
                return 10;
            }
            if (!fresh) {
                // Revalidate after restart/disconnect, including the hosted UTC
                // clock. This never renews the saved expiry or changes filters.
                const res = try self.poll(a);
                if (res.status != 200) return responseDelay(a, res);
                const latest = try p.parseClaim(a, res.body);
                const current = latest.value.request orelse {
                    try clearClaim(box);
                    return 10;
                };
                const old_json = try std.json.Stringify.valueAlloc(a, claim, .{});
                const new_json = try std.json.Stringify.valueAlloc(a, current, .{});
                if (!std.mem.eql(u8, old_json, new_json)) return error.ChangedClaim;
            }
            if (try box.read(a, "completion.json", 140 * 1024) == null) {
                const exe = try std.fs.selfExePathAlloc(a);
                var child = std.process.Child.init(&.{ exe, "--retained-log-worker", self.root }, a);
                child.stdin_behavior = .Ignore;
                child.stdout_behavior = .Ignore;
                child.stderr_behavior = .Ignore;
                try child.spawn();
                const term = try child.wait();
                if (try box.read(a, "completion.json", 140 * 1024) == null) {
                    const code: p.ErrorCode = if (term == .Signal and term.Signal == std.posix.SIG.ALRM) .query_timeout else .query_failed;
                    try saveError(a, box, claim, code);
                }
            }
            if (expiry <= std.time.microTimestamp() + 5_000000) {
                try clearClaim(box);
                return 10;
            }
            const body = (try box.read(a, "completion.json", 140 * 1024)).?;
            const path = try std.fmt.allocPrint(a, "/api/daemon/log-queries/{s}/complete", .{claim.id});
            const res = try self.post(a, path, body, 2048, expiry);
            if (res.status == 200) {
                const ack = try std.json.parseFromSlice(struct { status: []const u8, accepted: bool }, a, res.body, .{});
                if (!std.mem.eql(u8, ack.value.status, "completed") and !std.mem.eql(u8, ack.value.status, "failed") and !std.mem.eql(u8, ack.value.status, "expired")) return error.InvalidAck;
                try clearClaim(box);
                return 10;
            }
            if (res.status == 404) {
                try clearClaim(box);
                return 10;
            }
            if (res.status == 413) {
                try saveError(a, box, claim, .query_failed);
                return 10;
            }
            if (res.status == 422) {
                const err = try std.json.parseFromSlice(struct { @"error": []const u8 }, a, res.body, .{ .ignore_unknown_fields = true });
                if (std.mem.eql(u8, err.value.@"error", "stale_claim")) try clearClaim(box) else try saveError(a, box, claim, .query_failed);
                return 10;
            }
            return responseDelay(a, res);
        }
        const res = try self.poll(a);
        if (res.status != 200) return responseDelay(a, res);
        const parsed = try p.parseClaim(a, res.body);
        if (parsed.value.request) |claim| {
            const remaining = try claim.deadline() - std.time.microTimestamp();
            if (remaining <= 5_000000 or remaining > 60_000000) return error.ClockSkew;
            // Persist the exact response before executing. A lost response is
            // recovered by the next idempotent claim; a saved result is replayed.
            try box.remove("completion.json"); // Orphan left by terminal cleanup.
            try box.write("claim.json", res.body);
            return self.control(a, box, true);
        }
        return 10;
    }
    fn poll(self: *Worker, a: std.mem.Allocator) !http.Response {
        const body = try std.json.Stringify.valueAlloc(a, .{ .protocol_version = p.version, .daemon_version = build_options.version, .trace_id = true }, .{});
        const res = try self.post(a, "/api/daemon/log-queries/claim", body, 8192, null);
        if (res.status == 200) {
            const server_time = res.server_time orelse return error.ClockSkew;
            if (@abs(std.time.timestamp() - server_time) > 5) return error.ClockSkew;
        }
        return res;
    }
    fn upload(self: *Worker, a: std.mem.Allocator, box: *outbox.Outbox) !u32 {
        var it = box.dir.iterate();
        var names = std.ArrayList([]const u8){};
        var rows = std.array_list.Managed(std.json.Value).init(a);
        var payload: ?std.json.Value = null;
        var input_bytes: usize = 0;
        while (try it.next()) |entry| {
            if (!std.mem.startsWith(u8, entry.name, "upload-") or !std.mem.endsWith(u8, entry.name, ".json")) continue;
            const data = (try box.read(a, entry.name, 128 * 1024)) orelse continue;
            if (input_bytes + data.len > 2 * 1024 * 1024) break;
            const record = try std.json.parseFromSlice(std.json.Value, a, data, .{ .allocate = .alloc_always });
            const record_rows = record.value.object.get("logs").?.array.items;
            if (record_rows.len != 1) return error.InvalidBatch;
            var row = record_rows[0];
            const owned = try std.json.parseFromValue(logs.LogEntry, a, row, .{ .allocate = .alloc_always });
            var log = owned.value;
            try redact.redactLog(a, &log, null);
            if (!p.validString(log.message, std.math.maxInt(usize))) return error.InvalidRow;
            if (log.message.len > 4096) {
                var n: usize = 4096;
                while (!std.unicode.utf8ValidateSlice(log.message[0..n])) n -= 1;
                log.message = log.message[0..n];
                std.log.warn("protected upload message truncated; full retained row may be available, coverage unknown", .{});
            }
            try row.object.put("message", .{ .string = log.message });
            try row.object.put("source", .{ .string = log.source });
            try rows.append(row);
            try names.append(a, try a.dupe(u8, entry.name));
            if (payload == null) payload = record.value;
            input_bytes += data.len;
            if (rows.items.len == 100) break;
        }
        if (payload == null) return 10;
        try payload.?.object.put("logs", .{ .array = rows });
        try payload.?.object.put("daemon_version", .{ .string = build_options.version });
        const body = try std.json.Stringify.valueAlloc(a, payload.?, .{});
        const count = rows.items.len;
        const res = try self.post(a, "/api/ingest", body, 8192, null);
        if (res.status >= 200 and res.status < 300) {
            const ack = try std.json.parseFromSlice(std.json.Value, a, res.body, .{});
            if (ack.value != .object) return error.InvalidAck;
            const accepted = ack.value.object.get("log_count") orelse return error.InvalidAck;
            const rejected = ack.value.object.get("log_rejected_count") orelse return error.InvalidAck;
            if (accepted != .integer or rejected != .integer or accepted.integer != count or rejected.integer != 0) return error.InvalidAck;
            for (names.items) |name| try box.remove(name);
            return 10;
        }
        return responseDelay(a, res);
    }
    fn telemetry(self: *Worker, a: std.mem.Allocator, box: *outbox.Outbox) !u32 {
        const body = (try box.read(a, "telemetry.json", 2 * 1024 * 1024)) orelse return 10;
        const res = try self.post(a, "/api/ingest", body, 8192, null);
        if (res.status >= 200 and res.status < 300) {
            // Main owns rule parsing and sample counters. This bounded mailbox
            // transfers the last acknowledgment without sharing allocator state.
            try box.write("rules.json", res.body);
            // Telemetry is deliberately best-effort/latest-value. Even if main
            // replaces it during removal, no protected logs live in this slot.
            try box.remove("telemetry.json");
            return 10;
        }
        return responseDelay(a, res);
    }
    fn post(self: *Worker, a: std.mem.Allocator, path: []const u8, body: []const u8, cap: usize, expiry: ?i64) !http.Response {
        // All three streams share one ingestion-key allowance: at most one
        // request per two seconds, and a 429 suspends every stream.
        const wait_ms = self.last_request_at + 2000 - std.time.milliTimestamp();
        if (wait_ms > 0) std.Thread.sleep(@as(u64, @intCast(@min(wait_ms, 2000))) * std.time.ns_per_ms);
        if (expiry) |deadline| if (deadline <= std.time.microTimestamp() + 5_000000) return error.Expired;
        self.last_request_at = std.time.milliTimestamp();
        const res = try http.post(a, self.origin, self.key, path, body, cap);
        if (res.status == 429) self.rate_limit_until = std.time.milliTimestamp() + @as(i64, try responseDelay(a, res)) * 1000;
        return res;
    }
};
fn compatibleVersion() bool {
    if (build_options.version.len > 64) return false;
    const v = if (std.mem.startsWith(u8, build_options.version, "v")) build_options.version[1..] else build_options.version;
    const parsed = std.SemanticVersion.parse(v) catch return false;
    return parsed.pre == null and parsed.order(.{ .major = 0, .minor = 0, .patch = 2 }) != .lt;
}
fn jitter(seconds: u32) i64 {
    // Retry-After and endpoint-disable delays are minima, never jitter early.
    return @as(i64, seconds) * std.crypto.random.intRangeAtMost(i64, if (seconds >= 60) 1000 else 800, 1200);
}
fn responseDelay(a: std.mem.Allocator, res: http.Response) !u32 {
    return switch (res.status) {
        401 => 0, // Configuration is immutable in-process; restart reloads it.
        404, 422 => 300,
        429 => blk: {
            const body = try std.json.parseFromSlice(struct { retry_after_seconds: u32 = 60 }, a, res.body, .{ .ignore_unknown_fields = true });
            break :blk @max(body.value.retry_after_seconds, 60);
        },
        else => error.HttpFailed,
    };
}
fn clearClaim(box: *outbox.Outbox) !void {
    // Remove claim first: a crash cannot pair an old result with a new claim.
    try box.remove("claim.json");
    try box.remove("completion.json");
}
fn saveError(a: std.mem.Allocator, box: *outbox.Outbox, claim: p.Claim, code: p.ErrorCode) !void {
    const body = try std.json.Stringify.valueAlloc(a, .{ .protocol_version = p.version, .claim_token = claim.claim_token, .@"error" = @tagName(code) }, .{});
    try box.write("completion.json", body);
}
/// Called before ordinary daemon startup; only local parent supplies root.
/// Timer covers snapshot lock/setup, query, redaction and serialization.
pub fn childMain(root: []const u8) !void {
    var initial = c.itimerval{ .it_interval = .{ .tv_sec = 0, .tv_usec = 0 }, .it_value = .{ .tv_sec = 5, .tv_usec = 0 } };
    if (c.setitimer(c.ITIMER_REAL, &initial, null) != 0) return error.TimerFailed;
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var box = try outbox.Outbox.open(root);
    defer box.close();
    // A query child can briefly outlive a killed parent. A new daemon must
    // wait for that child, then reuse its result rather than run concurrently.
    const query_lock = try box.dir.createFile("query.lock", .{ .truncate = false, .mode = 0o600 });
    defer query_lock.close();
    try std.posix.flock(query_lock.handle, std.posix.LOCK.EX);
    if (try box.read(a, "completion.json", 140 * 1024) != null) return;
    const bytes = (try box.read(a, "claim.json", 8192)) orelse return error.MissingClaim;
    const parsed = try p.parseClaim(a, bytes);
    const claim = parsed.value.request orelse return error.MissingClaim;
    const remaining = try claim.deadline() - std.time.microTimestamp();
    if (remaining <= 5_000000 or remaining > 60_000000) return error.Expired;
    // Do not reset the local deadline after time already spent opening spool.
    var elapsed: c.itimerval = undefined;
    if (c.getitimer(c.ITIMER_REAL, &elapsed) != 0) return error.TimerFailed;
    // Reserve the accepted five-second UTC skew; a slow local clock must not
    // extend a hosted deadline. Near-expiry work is conservatively discarded.
    const budget = @min(remaining - 5_000000, elapsed.it_value.tv_sec * 1000000 + elapsed.it_value.tv_usec);
    initial.it_value = .{ .tv_sec = @intCast(@divTrunc(budget, 1000000)), .tv_usec = @intCast(@mod(budget, 1000000)) };
    if (c.setitimer(c.ITIMER_REAL, &initial, null) != 0) return error.TimerFailed;
    var query = pq.initRetainedLogQuery(a, root) catch {
        try saveError(a, &box, claim, .store_unavailable);
        return;
    };
    defer query.deinit();
    const bounds = query.logBounds() catch {
        try saveError(a, &box, claim, .store_unavailable);
        return;
    };
    const entries = query.retainedLogs(claim.filters) catch {
        try saveError(a, &box, claim, .query_failed);
        return;
    };
    const result = prepareResult(a, entries, claim.filters, bounds) catch |err| {
        try saveError(a, &box, claim, if (err == error.RedactionFailed) .redaction_failed else .query_failed);
        return;
    };
    const body = try std.json.Stringify.valueAlloc(a, .{ .protocol_version = p.version, .claim_token = claim.claim_token, .result = result }, .{});
    if (body.len > 140 * 1024) return error.ResultTooLarge;
    if (try claim.deadline() <= std.time.microTimestamp() + 5_000000) return error.Expired;
    try box.write("completion.json", body);
}
pub fn prepareResult(a: std.mem.Allocator, entries: []logs.LogEntry, f: p.Filters, bounds: [2]?i64) !p.Result {
    try f.validate();
    redact.redactLogs(a, entries, null) catch return error.RedactionFailed;
    var rows = std.ArrayList(p.Row){};
    var truncated = entries.len > f.max_rows;
    var bytes: usize = 2; // Array delimiters, plus per-row commas below.
    for (entries[0..@min(entries.len, f.max_rows)]) |e| {
        if (!p.validString(e.source, 255) or !p.validString(e.message, std.math.maxInt(usize)) or e.priority > 7) return error.InvalidRow;
        inline for (.{ e.unit, e.identifier, e.systemd_unit }) |s| if (s) |v| {
            if (!p.validString(v, 255)) return error.InvalidRow;
        };
        if (e.trace_id) |t| if (!p.validTrace(t)) return error.InvalidRow;
        var message = e.message;
        if (message.len > 4096) {
            var n: usize = 4096;
            while (!std.unicode.utf8ValidateSlice(message[0..n])) n -= 1;
            message = message[0..n];
            truncated = true;
        }
        const row = p.Row{ .timestamp = try p.formatTimestamp(a, e.timestamp), .source = e.source, .unit = e.unit, .identifier = e.identifier, .systemd_unit = e.systemd_unit, .priority = e.priority, .message = message, .trace_id = e.trace_id };
        const encoded = try std.json.Stringify.valueAlloc(a, row, .{});
        const size = encoded.len + @intFromBool(rows.items.len != 0);
        if (bytes + size > f.max_bytes) {
            truncated = true;
            break;
        }
        bytes += size;
        try rows.append(a, row);
    }
    return .{ .rows = try rows.toOwnedSlice(a), .truncated = truncated, .coverage = .{
        .snapshot_at = try p.formatTimestamp(a, std.time.timestamp()),
        .oldest_available_at = if (bounds[0]) |t| try p.formatTimestamp(a, t) else null,
        .newest_available_at = if (bounds[1]) |t| try p.formatTimestamp(a, t) else null,
    } };
}

test "outbound redaction precedes compact byte cap and UTF8 message cap" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const f = p.Filters{ .since = "1970-01-01T00:00:00Z", .until = "1970-01-01T00:00:10Z" };
    var entries = [_]logs.LogEntry{.{ .timestamp = 1, .source = try a.dupe(u8, "systemd"), .unit = null, .identifier = null, .systemd_unit = null, .priority = 3, .message = try a.dupe(u8, "password=secret-value email=user@example.com"), .pid = null }};
    const result = try prepareResult(a, &entries, f, .{ 1, 2 });
    try std.testing.expect(std.mem.indexOf(u8, result.rows[0].message, "secret-value") == null);
    try std.testing.expect(!result.coverage.complete);
    try std.testing.expectEqualStrings("unknown", result.coverage.gaps[0]);
    var large = std.ArrayList(u8){};
    for (0..1500) |_| try large.appendSlice(a, "☃");
    entries[0].message = try large.toOwnedSlice(a);
    const capped = try prepareResult(a, &entries, f, .{ null, null });
    try std.testing.expect(capped.truncated);
    try std.testing.expectEqual(@as(usize, 4095), capped.rows[0].message.len);
    var tiny = f;
    tiny.max_bytes = 1024;
    const omitted = try prepareResult(a, &entries, tiny, .{ null, null });
    try std.testing.expect(omitted.truncated);
    try std.testing.expectEqual(@as(usize, 0), omitted.rows.len);
    const empty = try prepareResult(a, &.{}, f, .{ null, null });
    try std.testing.expect(!empty.coverage.complete);
    try std.testing.expect(empty.coverage.oldest_available_at == null);
}

test "redaction allocation failure never produces raw rows" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var entries = [_]logs.LogEntry{.{ .timestamp = 1, .source = try a.dupe(u8, "systemd"), .unit = null, .identifier = null, .systemd_unit = null, .priority = 3, .message = try a.dupe(u8, "password=secret-value"), .pid = null }};
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.RedactionFailed, prepareResult(failing.allocator(), &entries, .{ .since = "1970-01-01T00:00:00Z", .until = "1970-01-01T00:00:10Z" }, .{ null, null }));
}

test "control status handling and jitter preserve required minimum delays" {
    std.testing.refAllDecls(http);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try std.testing.expectEqual(@as(u32, 0), try responseDelay(a, .{ .status = 401, .body = "{}" }));
    try std.testing.expectEqual(@as(u32, 300), try responseDelay(a, .{ .status = 404, .body = "{}" }));
    try std.testing.expectEqual(@as(u32, 300), try responseDelay(a, .{ .status = 422, .body = "{}" }));
    try std.testing.expectEqual(@as(u32, 120), try responseDelay(a, .{ .status = 429, .body = "{\"retry_after_seconds\":120}" }));
    try std.testing.expectError(error.HttpFailed, responseDelay(a, .{ .status = 503, .body = "{}" }));
    for (0..100) |_| {
        const idle = jitter(10);
        try std.testing.expect(idle >= 8000 and idle <= 12000);
        try std.testing.expect(jitter(60) >= 60000);
        try std.testing.expect(jitter(300) >= 300000);
    }
}
