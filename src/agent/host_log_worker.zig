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
const auth_backoff_min_s = 300;
const auth_backoff_max_s = 3600;
const max_upload_batch = 100;
const reconcile_interval_ms = 5 * 60 * 1000;
const log_interval_ms = 10 * 60 * 1000;

/// The hosted limit is policy, not a boundary this process may rely on: every
/// accepted query takes the shared roll lock for up to five seconds.
pub const QueryBudget = struct {
    pub const capacity = 10;
    const refill_ms = 3600 * 1000 / capacity;
    tokens: u32 = capacity,
    refilled_at: i64 = 0,

    pub fn take(self: *QueryBudget, now_ms: i64) bool {
        if (self.refilled_at == 0) self.refilled_at = now_ms;
        const earned = @divFloor(now_ms - self.refilled_at, refill_ms);
        if (earned > 0) {
            self.tokens = @intCast(@min(@as(i64, capacity), self.tokens + earned));
            self.refilled_at += earned * refill_ms;
        }
        if (self.tokens == 0) return false;
        self.tokens -= 1;
        return true;
    }
};

pub const Worker = struct {
    root: []const u8,
    origin: []const u8,
    key: []const u8,
    shared: *outbox.Shared,
    /// Absolute config file path when it supplied both credentials; null when
    /// the command line did, because then there is nothing to re-read.
    credentials_path: ?[]const u8 = null,
    reloaded: ?std.json.Parsed(Credentials) = null,
    stop: std.atomic.Value(bool) = .init(false),
    thread: ?std.Thread = null,
    rate_limit_until: i64 = 0,
    last_request_at: i64 = 0,
    auth_failed: bool = false,
    auth_retry_at: i64 = 0,
    auth_backoff_s: u32 = auth_backoff_min_s,
    upload_batch: usize = max_upload_batch,
    query_budget: QueryBudget = .{},
    poll_after_s: u32 = 10,
    last_skew_log: i64 = 0,
    last_spool_log: i64 = 0,

    const Credentials = struct { server_url: ?[]const u8 = null, api_key: ?[]const u8 = null };

    pub fn start(self: *Worker) !void {
        self.thread = try std.Thread.spawn(.{}, run, .{self});
    }
    pub fn deinit(self: *Worker) void {
        self.stop.store(true, .release);
        if (self.thread) |t| t.join();
        if (self.reloaded) |r| r.deinit();
    }
    fn run(self: *Worker) void {
        var box = outbox.Outbox.open(self.root, self.shared) catch |err| {
            std.log.err("control worker disabled: cannot open outbox: {}; uploads and log queries will not run", .{err});
            return;
        };
        defer box.close();
        // Prevent two daemon instances executing or delivering the same claim.
        const lock = box.dir.createFile("worker.lock", .{ .truncate = false, .mode = 0o600 }) catch |err| {
            std.log.err("control worker disabled: cannot create worker.lock: {}", .{err});
            return;
        };
        defer lock.close();
        std.posix.flock(lock.handle, std.posix.LOCK.EX | std.posix.LOCK.NB) catch |err| {
            std.log.err("control worker disabled: worker.lock is held ({}); is another sermon-agent using this root?", .{err});
            return;
        };
        var next_control: i64 = 0;
        var next_upload: i64 = 0;
        var next_telemetry: i64 = 0;
        var next_reconcile: i64 = std.time.milliTimestamp() + reconcile_interval_ms;
        var backoff: u32 = 2;
        var upload_backoff: u32 = 2;
        var telemetry_backoff: u32 = 2;
        while (!self.stop.load(.acquire)) {
            var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer arena.deinit();
            const a = arena.allocator();
            const now = std.time.milliTimestamp();
            if (now >= next_reconcile) {
                box.reconcile() catch |err| self.spoolError("reconcile", err);
                next_reconcile = now + reconcile_interval_ms;
            }
            // A rejected key pauses claims and protected uploads on a slow
            // backoff. Telemetry keeps its per-cycle cadence and is the probe
            // that notices a repaired key first.
            const authorized = !self.auth_failed or now >= self.auth_retry_at;
            if (authorized and now >= self.rate_limit_until and now >= next_control and compatibleVersion()) {
                self.reloadCredentials(a, &box);
                if (self.control(a, &box, false)) |delay| {
                    backoff = 2;
                    next_control = std.time.milliTimestamp() + jitter(delay);
                } else |err| {
                    if (err == error.ClockSkew) self.logClockSkew();
                    next_control = std.time.milliTimestamp() + jitter(backoff);
                    backoff = @min(backoff * 2, 60);
                }
            }
            if (authorized and now >= self.rate_limit_until and now >= next_upload) {
                self.reloadCredentials(a, &box);
                if (self.upload(a, &box)) |delay| {
                    upload_backoff = 2;
                    next_upload = std.time.milliTimestamp() + jitter(delay);
                } else |err| {
                    std.log.warn("log upload unacknowledged ({}): durable records retained for retry", .{err});
                    next_upload = std.time.milliTimestamp() + jitter(upload_backoff);
                    upload_backoff = @min(upload_backoff * 2, 60);
                }
            }
            if (now >= self.rate_limit_until and now >= next_telemetry) {
                self.reloadCredentials(a, &box);
                if (self.telemetry(a, &box)) |delay| {
                    telemetry_backoff = 2;
                    next_telemetry = std.time.milliTimestamp() + jitter(delay);
                } else |_| {
                    next_telemetry = std.time.milliTimestamp() + jitter(telemetry_backoff);
                    telemetry_backoff = @min(telemetry_backoff * 2, 60);
                }
            }
            std.Thread.sleep(100 * std.time.ns_per_ms);
        }
    }
    /// Only after a 401. A changed key goes through `bind`, so records spooled
    /// under the old binding are held rather than sent to a different server.
    fn reloadCredentials(self: *Worker, a: std.mem.Allocator, box: *outbox.Outbox) void {
        if (!self.auth_failed) return;
        const path = self.credentials_path orelse return;
        const bytes = std.fs.cwd().readFileAlloc(a, path, 4096) catch return;
        defer a.free(bytes);
        const parsed = std.json.parseFromSlice(Credentials, std.heap.page_allocator, bytes, .{ .ignore_unknown_fields = true, .allocate = .alloc_always }) catch return;
        const url = parsed.value.server_url orelse return parsed.deinit();
        const key = parsed.value.api_key orelse return parsed.deinit();
        if (std.mem.eql(u8, url, self.origin) and std.mem.eql(u8, key, self.key)) return parsed.deinit();
        box.bind(a, url, key) catch |err| {
            self.spoolError("rebind", err);
            return parsed.deinit();
        };
        if (self.reloaded) |old| old.deinit();
        self.reloaded = parsed;
        self.origin = url;
        self.key = key;
        self.auth_retry_at = 0;
        std.log.info("ingestion credentials reloaded from config after 401", .{});
    }
    fn noteStatus(self: *Worker, status: u16) void {
        if (status == 401) {
            if (!self.auth_failed) std.log.warn("ingestion key rejected (401): claims and log uploads retry every {d}s up to {d}s; telemetry keeps probing", .{ auth_backoff_min_s, auth_backoff_max_s });
            const now = std.time.milliTimestamp();
            if (!self.auth_failed or now >= self.auth_retry_at) {
                self.auth_retry_at = now + jitter(self.auth_backoff_s);
                self.auth_backoff_s = @min(self.auth_backoff_s * 2, auth_backoff_max_s);
            }
            self.auth_failed = true;
        } else if (status >= 200 and status < 300 and self.auth_failed) {
            std.log.info("ingestion key accepted again: claims and log uploads resumed", .{});
            self.auth_failed = false;
            self.auth_retry_at = 0;
            self.auth_backoff_s = auth_backoff_min_s;
        }
    }
    fn logClockSkew(self: *Worker) void {
        const now = std.time.milliTimestamp();
        if (self.last_skew_log != 0 and now - self.last_skew_log < log_interval_ms) return;
        self.last_skew_log = now;
        std.log.warn("retained-log queries refused: local clock and hosted clock disagree by more than 5s, or a claim deadline is implausible; fix NTP", .{});
    }
    fn spoolError(self: *Worker, what: []const u8, err: anyerror) void {
        _ = outbox.counters.spool_errors.fetchAdd(1, .monotonic);
        const now = std.time.milliTimestamp();
        if (self.last_spool_log != 0 and now - self.last_spool_log < log_interval_ms) return;
        self.last_spool_log = now;
        std.log.err("outbox {s} failed: {}", .{ what, err });
    }
    fn control(self: *Worker, a: std.mem.Allocator, box: *outbox.Outbox, fresh: bool) !u32 {
        const idle = self.poll_after_s;
        if (try box.read(a, "claim.json", 8192)) |saved| {
            const parsed = try p.parseClaim(a, saved);
            const claim = parsed.value.request orelse return error.InvalidClaim;
            const expiry = try claim.deadline();
            const now = std.time.microTimestamp();
            if (expiry <= now + 5_000000 or expiry - now > 60_000000) {
                try clearClaim(box);
                return idle;
            }
            if (!fresh) {
                // Revalidate after restart/disconnect, including the hosted UTC
                // clock. This never renews the saved expiry or changes filters.
                const res = try self.poll(a);
                if (res.status != 200) return responseDelay(a, res);
                const latest = try p.parseClaim(a, res.body);
                const current = latest.value.request orelse {
                    try clearClaim(box);
                    return idle;
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
                return idle;
            }
            const body = (try box.read(a, "completion.json", 140 * 1024)).?;
            const path = try std.fmt.allocPrint(a, "/api/daemon/log-queries/{s}/complete", .{claim.id});
            const res = try self.post(a, path, body, 2048, expiry);
            if (res.status == 200) {
                const ack = try std.json.parseFromSlice(struct { status: []const u8, accepted: bool }, a, res.body, .{});
                if (!std.mem.eql(u8, ack.value.status, "completed") and !std.mem.eql(u8, ack.value.status, "failed") and !std.mem.eql(u8, ack.value.status, "expired")) return error.InvalidAck;
                try clearClaim(box);
                return idle;
            }
            if (res.status == 404) {
                try clearClaim(box);
                return idle;
            }
            if (res.status == 413) {
                try saveError(a, box, claim, .query_failed);
                return idle;
            }
            if (res.status == 422) {
                const err = try std.json.parseFromSlice(struct { @"error": []const u8 }, a, res.body, .{ .ignore_unknown_fields = true });
                if (std.mem.eql(u8, err.value.@"error", "stale_claim")) try clearClaim(box) else try saveError(a, box, claim, .query_failed);
                return idle;
            }
            return responseDelay(a, res);
        }
        const res = try self.poll(a);
        if (res.status != 200) return responseDelay(a, res);
        const parsed = try p.parseClaim(a, res.body);
        self.poll_after_s = parsed.value.poll_after_seconds;
        if (parsed.value.request) |claim| {
            const remaining = try claim.deadline() - std.time.microTimestamp();
            if (remaining <= 5_000000 or remaining > 60_000000) return error.ClockSkew;
            // Persist the exact response before executing. A lost response is
            // recovered by the next idempotent claim; a saved result is replayed.
            try box.remove("completion.json"); // Orphan left by terminal cleanup.
            try box.write("claim.json", res.body);
            if (!self.query_budget.take(std.time.milliTimestamp())) {
                // Answer without spawning the query child, so an over-eager or
                // compromised hosted side cannot keep the roll lock busy.
                std.log.warn("retained-log query refused: local limit of {d} per hour exceeded", .{QueryBudget.capacity});
                try saveError(a, box, claim, .query_failed);
            }
            return self.control(a, box, true);
        }
        return self.poll_after_s;
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
        var poison = std.ArrayList([]const u8){};
        var rows = std.array_list.Managed(std.json.Value).init(a);
        var payload: ?std.json.Value = null;
        var input_bytes: usize = 0;
        while (try it.next()) |entry| {
            if (!std.mem.startsWith(u8, entry.name, "upload-") or !std.mem.endsWith(u8, entry.name, ".json")) continue;
            const name = try a.dupe(u8, entry.name);
            const data = box.read(a, name, 128 * 1024) catch |err| switch (err) {
                error.FileTooBig => {
                    try poison.append(a, name);
                    continue;
                },
                else => return err,
            } orelse continue;
            if (input_bytes + data.len > 2 * 1024 * 1024) break;
            const record = loadRecord(a, data) catch |err| switch (err) {
                // Fail closed and retry: these say nothing about the record.
                error.OutOfMemory, error.RedactionFailed => return err,
                else => {
                    try poison.append(a, name);
                    continue;
                },
            };
            try rows.append(record.row);
            try names.append(a, name);
            if (payload == null) payload = record.payload;
            input_bytes += data.len;
            if (rows.items.len >= self.upload_batch) break;
        }
        // After iteration: renames must not disturb the directory walk.
        for (poison.items) |name| {
            std.log.err("undeliverable upload record quarantined as {s}.bad; the retained local store still has the row", .{name});
            try box.quarantine(a, name);
        }
        if (payload == null) return 10;
        try payload.?.object.put("logs", .{ .array = rows });
        try payload.?.object.put("daemon_version", .{ .string = build_options.version });
        const body = try std.json.Stringify.valueAlloc(a, payload.?, .{});
        const res = try self.post(a, "/api/ingest", body, 8192, null);
        switch (classifyUpload(a, res, rows.items.len)) {
            .delivered => {
                for (names.items) |name| try box.removeUpload(name);
                // Stay narrow while isolating a refused row; widen once the
                // backlog that contained it has drained.
                if (names.items.len < self.upload_batch) self.upload_batch = max_upload_batch;
                return 10;
            },
            // Hosted answered and will not take this batch as it stands, and
            // does not say which row. Replaying it duplicates the rows it did
            // accept, so bisect: halve until the refused record is alone, then
            // set only that record aside. Retries are bounded by log2(batch).
            .rejected => {
                if (names.items.len == 1) {
                    std.log.err("hosted refused upload record {s} (status {d}): quarantined", .{ names.items[0], res.status });
                    try box.quarantine(a, names.items[0]);
                    self.upload_batch = max_upload_batch;
                } else {
                    std.log.warn("hosted refused part of a {d}-record upload (status {d}): retrying in smaller batches", .{ names.items.len, res.status });
                    self.upload_batch = names.items.len / 2;
                }
                return 10;
            },
            .unacknowledged => return error.InvalidAck,
            .other => return responseDelay(a, res),
        }
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
        if (res.status == 401) return 10; // Per-cycle probe, as before the worker existed.
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
        self.noteStatus(res.status);
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
        // 401 is paced by Worker.auth_retry_at; this is only the stream's floor.
        401, 404, 422 => 300,
        429 => blk: {
            const body = try std.json.parseFromSlice(struct { retry_after_seconds: u32 = 60 }, a, res.body, .{ .ignore_unknown_fields = true });
            break :blk @max(body.value.retry_after_seconds, 60);
        },
        else => error.HttpFailed,
    };
}
const UploadOutcome = enum { delivered, rejected, unacknowledged, other };
/// A 2xx without both counters is a hosted build that predates acknowledged
/// delivery: keep the records. Counters that disagree are an explicit refusal.
fn classifyUpload(a: std.mem.Allocator, res: http.Response, count: usize) UploadOutcome {
    if (res.status == 400 or res.status == 413 or res.status == 422) return .rejected;
    if (res.status < 200 or res.status >= 300) return .other;
    const ack = std.json.parseFromSlice(std.json.Value, a, res.body, .{}) catch return .unacknowledged;
    if (ack.value != .object) return .unacknowledged;
    const accepted = ack.value.object.get("log_count") orelse return .unacknowledged;
    const rejected = ack.value.object.get("log_rejected_count") orelse return .unacknowledged;
    if (accepted != .integer or rejected != .integer) return .unacknowledged;
    return if (accepted.integer == count and rejected.integer == 0) .delivered else .rejected;
}
const Record = struct { payload: std.json.Value, row: std.json.Value };
/// Spool contents are local but not trusted: a torn disk, an older daemon or an
/// operator edit must surface as an error, never a panic.
fn loadRecord(a: std.mem.Allocator, data: []const u8) !Record {
    const record = std.json.parseFromSlice(std.json.Value, a, data, .{ .allocate = .alloc_always }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidRecord;
    if (record.value != .object) return error.InvalidRecord;
    const list = record.value.object.get("logs") orelse return error.InvalidRecord;
    if (list != .array or list.array.items.len != 1 or list.array.items[0] != .object) return error.InvalidRecord;
    var row = list.array.items[0];
    const owned = std.json.parseFromValue(logs.LogEntry, a, row, .{ .allocate = .alloc_always }) catch |err| return if (err == error.OutOfMemory) err else error.InvalidRecord;
    var log = owned.value;
    redact.redactLog(a, &log, null) catch return error.RedactionFailed;
    if (!p.validString(log.source, 255) or !p.validString(log.message, std.math.maxInt(usize)) or log.priority > 7) return error.InvalidRow;
    if (log.message.len > 4096) {
        var n: usize = 4096;
        while (!std.unicode.utf8ValidateSlice(log.message[0..n])) n -= 1;
        log.message = log.message[0..n];
        std.log.warn("protected upload message truncated; full retained row may be available, coverage unknown", .{});
    }
    try row.object.put("message", .{ .string = log.message });
    try row.object.put("source", .{ .string = log.source });
    return .{ .payload = record.value, .row = row };
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
    var shared = outbox.Shared{}; // The child never enqueues; the cache is unused.
    var box = try outbox.Outbox.open(root, &shared);
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
fn representable(e: logs.LogEntry) bool {
    if (!p.validString(e.source, 255) or !p.validString(e.message, std.math.maxInt(usize)) or e.priority > 7) return false;
    inline for (.{ e.unit, e.identifier, e.systemd_unit }) |s| if (s) |v| {
        if (!p.validString(v, 255)) return false;
    };
    if (e.trace_id) |t| if (!p.validTrace(t)) return false;
    return true;
}
pub fn prepareResult(a: std.mem.Allocator, entries: []logs.LogEntry, f: p.Filters, bounds: [2]?i64) !p.Result {
    try f.validate();
    redact.redactLogs(a, entries, null) catch return error.RedactionFailed;
    var rows = std.ArrayList(p.Row){};
    var truncated = entries.len > f.max_rows;
    var bytes: usize = 2; // Array delimiters, plus per-row commas below.
    for (entries[0..@min(entries.len, f.max_rows)]) |e| {
        // One unrepresentable stored row must not fail the whole window. It is
        // omitted and reported as missing detail, never sent malformed.
        if (!representable(e)) {
            truncated = true;
            continue;
        }
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
    try std.testing.expectEqual(@as(u32, 300), try responseDelay(a, .{ .status = 401, .body = "{}" }));
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

test "poison spool records are errors, never panics, and empty rows do not fail a query" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_][]const u8{
        "",
        "not json",
        "[]",
        "{}",
        "{\"logs\":null}",
        "{\"logs\":[]}",
        "{\"logs\":[1]}",
        "{\"logs\":[{\"timestamp\":\"1\"}]}",
    }) |bytes| try std.testing.expectError(error.InvalidRecord, loadRecord(a, bytes));
    const row = "{{\"collected_at\":1,\"logs\":[{{\"timestamp\":1,\"source\":\"systemd\",\"unit\":null,\"identifier\":null,\"systemd_unit\":null,\"priority\":3,\"message\":\"{s}\",\"pid\":null,\"trace_id\":null}}]}}";
    try std.testing.expectError(error.InvalidRow, loadRecord(a, try std.fmt.allocPrint(a, row, .{""})));
    try std.testing.expectError(error.InvalidRow, loadRecord(a, try std.fmt.allocPrint(a, row, .{"nul\\u0000byte"})));
    const ok = try loadRecord(a, try std.fmt.allocPrint(a, row, .{"password=secret-value"}));
    try std.testing.expect(std.mem.indexOf(u8, ok.row.object.get("message").?.string, "secret-value") == null);

    var entries = [_]logs.LogEntry{
        .{ .timestamp = 2, .source = try a.dupe(u8, "systemd"), .unit = null, .identifier = null, .systemd_unit = null, .priority = 3, .message = try a.dupe(u8, ""), .pid = null },
        .{ .timestamp = 1, .source = try a.dupe(u8, "systemd"), .unit = null, .identifier = null, .systemd_unit = null, .priority = 3, .message = try a.dupe(u8, "kept"), .pid = null },
    };
    const result = try prepareResult(a, &entries, .{ .since = "1970-01-01T00:00:00Z", .until = "1970-01-01T00:00:10Z" }, .{ 1, 2 });
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("kept", result.rows[0].message);
    try std.testing.expect(result.truncated);
}

test "upload acknowledgment separates delivered, refused and pre-acknowledgment hosted builds" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const t = std.testing;
    try t.expectEqual(UploadOutcome.delivered, classifyUpload(a, .{ .status = 200, .body = "{\"log_count\":2,\"log_rejected_count\":0}" }, 2));
    try t.expectEqual(UploadOutcome.rejected, classifyUpload(a, .{ .status = 200, .body = "{\"log_count\":1,\"log_rejected_count\":1}" }, 2));
    try t.expectEqual(UploadOutcome.rejected, classifyUpload(a, .{ .status = 202, .body = "{\"log_count\":1,\"log_rejected_count\":0}" }, 2));
    try t.expectEqual(UploadOutcome.rejected, classifyUpload(a, .{ .status = 422, .body = "{}" }, 2));
    try t.expectEqual(UploadOutcome.rejected, classifyUpload(a, .{ .status = 413, .body = "" }, 2));
    try t.expectEqual(UploadOutcome.unacknowledged, classifyUpload(a, .{ .status = 200, .body = "{\"log_count\":2}" }, 2));
    try t.expectEqual(UploadOutcome.unacknowledged, classifyUpload(a, .{ .status = 200, .body = "<html>" }, 2));
    try t.expectEqual(UploadOutcome.other, classifyUpload(a, .{ .status = 503, .body = "{}" }, 2));
    try t.expectEqual(UploadOutcome.other, classifyUpload(a, .{ .status = 401, .body = "{}" }, 2));
}

test "rejected key backs off slowly and recovers without a restart" {
    var shared = outbox.Shared{};
    var w = Worker{ .root = "", .origin = "", .key = "", .shared = &shared };
    w.noteStatus(401);
    try std.testing.expect(w.auth_failed);
    const first = w.auth_retry_at;
    try std.testing.expect(first >= std.time.milliTimestamp() + 290_000);
    w.noteStatus(401); // Other streams in the same pass must not compound it.
    try std.testing.expectEqual(first, w.auth_retry_at);
    try std.testing.expectEqual(@as(u32, 600), w.auth_backoff_s);
    w.auth_retry_at = 1;
    for (0..8) |_| {
        w.noteStatus(401);
        w.auth_retry_at = 1;
    }
    try std.testing.expectEqual(@as(u32, auth_backoff_max_s), w.auth_backoff_s);
    w.noteStatus(503);
    try std.testing.expect(w.auth_failed);
    w.noteStatus(200);
    try std.testing.expect(!w.auth_failed);
    try std.testing.expectEqual(@as(u32, auth_backoff_min_s), w.auth_backoff_s);
}

test "changed config credentials rebind the spool before they are used" {
    const a = std.testing.allocator;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const root = try temp.dir.realpathAlloc(a, ".");
    defer a.free(root);
    try temp.dir.writeFile(.{ .sub_path = "config.json", .data = "{\"interval\":10,\"server_url\":\"https://example.invalid\",\"api_key\":\"new-fixture-key\"}" });
    const path = try std.fs.path.join(a, &.{ root, "config.json" });
    defer a.free(path);
    var shared = outbox.Shared{};
    var box = try outbox.Outbox.open(root, &shared);
    defer box.close();
    try box.bind(a, "https://example.invalid", "old-fixture-key");
    try box.enqueue("spooled under the old binding");
    var w = Worker{ .root = root, .origin = "https://example.invalid", .key = "old-fixture-key", .shared = &shared, .credentials_path = path };
    defer w.deinit();
    w.reloadCredentials(a, &box);
    try std.testing.expectEqualStrings("old-fixture-key", w.key); // No 401 yet.
    w.noteStatus(401);
    w.reloadCredentials(a, &box);
    try std.testing.expectEqualStrings("new-fixture-key", w.key);
    try std.testing.expectEqual(@as(i64, 0), w.auth_retry_at);
    try std.testing.expectEqual(@as(usize, 0), box.usage().records);
}

test "local query budget refuses a burst beyond ten and refills over the hour" {
    var b = QueryBudget{};
    const t0: i64 = 1_000_000;
    for (0..QueryBudget.capacity) |_| try std.testing.expect(b.take(t0));
    try std.testing.expect(!b.take(t0));
    try std.testing.expect(!b.take(t0 + 359_000));
    try std.testing.expect(b.take(t0 + 360_000));
    try std.testing.expect(!b.take(t0 + 360_001));
    var n: usize = 0;
    while (b.take(t0 + 10 * 3600_000)) n += 1;
    try std.testing.expectEqual(@as(usize, QueryBudget.capacity), n);
}
