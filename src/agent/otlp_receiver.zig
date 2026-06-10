//! OTLP/HTTP local collector (plan 26).
//!
//! This module is the daemon-side OTLP receiver. The thesis-pure on-host path
//! is: apps emit OTLP to a localhost daemon receiver at full fidelity, the
//! daemon keeps it on-host (queryable over SSH like the existing /proc +
//! journald data), and forwards a sampled copy to the hosted /v1/* endpoints.
//!
//! SCOPE OF THIS FILE (plan 26 phases 1-2):
//!   - OTLP/JSON envelope structs for the *logs* signal.
//!   - `mapLogRecord`: a PURE mapping from one decoded OTLP/JSON LogRecord to
//!     the daemon's canonical on-host log representation (`logs.LogEntry`),
//!     so received OTLP logs flow through the same local-write + forward
//!     machinery as journald logs.
//!   - `Receiver` (phase 2): the single-threaded synchronous listen/accept
//!     loop on 127.0.0.1:receiver_port, run on its own thread and spawned only
//!     when `receiver_enabled = true`. Parses `POST /v1/logs`, maps each
//!     record, and hands the result to the `Forwarder` seam (the real one
//!     POSTs to the hosted /v1/logs with the Bearer otlp_token).
//!
//! DELIBERATELY NOT HERE YET (see docs/plans/26-daemon-otlp-local-collector.md):
//!   - On-host persistence into the parquet hot tier (phase 3, explicitly
//!     gated on re-validating roll sizing under external write volume).
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

/// OTLP AnyValue. A log body is an AnyValue, and SDKs do emit non-string bodies
/// (a numeric metric-ish event, a structured map). We capture every scalar
/// variant so a non-string body renders to real text instead of being silently
/// acked as an empty message. In OTLP/JSON, `intValue` is a STRING (int64 can
/// exceed JS-safe integers) and `bytesValue` is base64; `arrayValue`/`kvlistValue`
/// are nested - we don't decode their shape here (the nested structs are present
/// only so they parse), we render a clearly-marked placeholder so a structured
/// body is never mistaken for empty. See `renderAnyValue`.
pub const AnyValue = struct {
    stringValue: ?[]const u8 = null,
    intValue: ?[]const u8 = null,
    doubleValue: ?f64 = null,
    boolValue: ?bool = null,
    bytesValue: ?[]const u8 = null,
    arrayValue: ?ArrayValue = null,
    kvlistValue: ?KeyValueList = null,
};

/// OTLP `arrayValue` payload. Parsed (so a body carrying one doesn't fail) but
/// not structurally rendered - `renderAnyValue` emits a marked placeholder.
pub const ArrayValue = struct {
    values: []const AnyValue = &.{},
};

/// OTLP `kvlistValue` payload. Same treatment as `ArrayValue`.
pub const KeyValueList = struct {
    values: []const KeyValue = &.{},
};

/// Render an OTLP `AnyValue` body to an OWNED text log message (duped from
/// `allocator`; caller frees). Scalars (string/int/double/bool) become their
/// text form; `bytesValue` and the nested `arrayValue`/`kvlistValue` get a
/// clearly-marked `[otlp <kind>]` placeholder so a structured or binary body is
/// never silently acked as an empty message. Always returns owned memory so the
/// caller has one free path regardless of variant.
pub fn renderAnyValue(allocator: Allocator, value: AnyValue) ![]u8 {
    if (value.stringValue) |s| return allocator.dupe(u8, s);
    if (value.intValue) |s| return allocator.dupe(u8, s); // OTLP/JSON int64 is a string.
    if (value.boolValue) |b| return allocator.dupe(u8, if (b) "true" else "false");
    if (value.doubleValue) |d| return std.fmt.allocPrint(allocator, "{d}", .{d});
    if (value.bytesValue != null) return allocator.dupe(u8, "[otlp bytesValue]");
    if (value.arrayValue != null) return allocator.dupe(u8, "[otlp arrayValue]");
    if (value.kvlistValue != null) return allocator.dupe(u8, "[otlp kvlistValue]");
    return allocator.dupe(u8, ""); // genuinely empty body (no variant set).
}

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
/// The same goes for a value that parses but whose seconds overflow i64: this
/// field is producer-controlled, and an unchecked cast here would panic - and
/// a panic aborts the whole daemon, not just this request.
pub fn unixSecondsFromNano(time_unix_nano: ?[]const u8, now: i64) i64 {
    const s = time_unix_nano orelse return now;
    if (s.len == 0) return now;
    const nanos = std.fmt.parseInt(i128, s, 10) catch return now;
    return std.math.cast(i64, @divTrunc(nanos, std.time.ns_per_s)) orelse now;
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
    const source = try allocator.dupe(u8, "otlp");
    errdefer allocator.free(source);

    // renderAnyValue owns its result and maps every AnyValue variant - including
    // non-string bodies - to real text (or a marked placeholder), so a numeric/
    // bool/structured body is never silently acked as an empty message.
    const message = if (record.body) |body| try renderAnyValue(allocator, body) else try allocator.dupe(u8, "");
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

// ── Receiver (plan 26 phase 2): std.http.Server listen/accept loop ──
//
// A single-threaded SYNCHRONOUS accept loop on 127.0.0.1:receiver_port, run on
// its own std.Thread and spawned ONLY when receiver_enabled = true. It accepts
// one connection at a time, reads a `POST /v1/logs` OTLP/JSON body (bounded
// size), parses it into `LogsData`, maps each record via `mapLogRecord`, and
// hands the results to the forward seam (`Forwarder`; the real one mirrors
// push.zig's client and POSTs to the hosted /v1/logs with a Bearer otlp_token).
// On-host persistence is plan 26 phase 3, gated on roll-sizing revalidation.
//
// CONCURRENCY TRADEOFF (intentional for v1): one synchronous accept loop
// serializes producers; a hung client or slow forward blocks others. Acceptable
// because same-host SDKs batch/retry and forwarding will be decoupled from
// accept once persistence lands. The per-connection read deadline
// (`read_timeout_s`) bounds how long any one client can hold the loop. See
// docs/plans/26-daemon-otlp-local-collector.md "Architecture".

/// Request body cap. Precedent ceilings in this codebase: the config loader
/// reads into a fixed 4 KiB buffer, log_rules.json is capped at 256 KiB
/// (max_rules_file_bytes), and real OTLP SDK batches are tens-to-hundreds of
/// KiB - so 1 MiB comfortably fits honest producers while bounding what a
/// misbehaving local process can make the daemon buffer per request.
pub const max_body_bytes: usize = 1024 * 1024;

/// SO_RCVTIMEO on each accepted connection. A producer that connects and then
/// stops sending mid-request would otherwise block the single-threaded accept
/// loop forever; with the deadline the blocked read errors out and the
/// connection is dropped, so the loop always returns to accept().
pub const read_timeout_s: isize = 10;

/// The forward seam. The receiver hands each request's mapped entries to this
/// function pointer so tests can fake the forwarder; main.zig wires in the
/// real hosted forwarder (`HostedForwarder`) or `drop_forwarder` when the
/// hosted credentials are missing. `arena` is the per-request arena - anything
/// the forwarder allocates from it dies with the request, and `entries` are
/// only valid for the duration of the call.
pub const Forwarder = struct {
    ctx: ?*anyopaque = null,
    forwardFn: *const fn (ctx: ?*anyopaque, arena: Allocator, entries: []const logs_mod.LogEntry) anyerror!void,

    pub fn forward(f: Forwarder, arena: Allocator, entries: []const logs_mod.LogEntry) anyerror!void {
        return f.forwardFn(f.ctx, arena, entries);
    }
};

/// "Forwarding not configured" seam, wired in when server_url or otlp_token is
/// missing. It FAILS every forward (error.OtlpForwardingNotConfigured) rather
/// than silently succeeding: phase 2 has no on-host persistence (phase 3, gated)
/// so this build can do nothing useful with a received record it cannot forward.
/// Failing makes handleRequest answer 503, which tells the operator the receiver
/// is enabled-but-unwired instead of silently accepting-and-dropping telemetry.
pub const drop_forwarder = Forwarder{ .forwardFn = dropForward };

fn dropForward(_: ?*anyopaque, _: Allocator, _: []const logs_mod.LogEntry) anyerror!void {
    return error.OtlpForwardingNotConfigured;
}

/// SO_SNDTIMEO/SO_RCVTIMEO on the forward socket. The forward runs INLINE on the
/// single-threaded accept loop before the local 200/503, so a hung hosted call
/// (slow TLS handshake, a peer that accepts then never replies) would otherwise
/// wedge the loop - and stop()'s join - indefinitely. With the deadline a stuck
/// send/recv errors out, the forward fails (-> 503, the SDK retries), and the
/// loop returns to accept. CONSTRAINT: std.http.Client.fetch exposes no timeout
/// knob (verified against std/http/Client.zig in zig 0.15.2: FetchOptions has no
/// timeout field, and fetch connects + sends + receives internally). So we
/// pre-connect via client.connect, set the socket deadline on that connection's
/// fd, then drive client.request over it. This bounds the request write and the
/// response read; the connect() itself (DNS + TCP + TLS handshake) is bounded
/// only by the kernel's connect timeout, not this value - acceptable for a v1
/// inline forward, and removed entirely once forwarding is decoupled from accept.
pub const forward_timeout_s: isize = 10;

/// Forwards mapped records to the hosted OTLP receiver (sermon-web plan 23):
/// POST <server_url>/v1/logs with `Authorization: Bearer <otlp_token>`. The body
/// is a RE-SERIALIZED minimal OTLP/JSON envelope built from the MAPPED records -
/// not the raw received bytes - so the future sampling/redaction gate applies on
/// the way out (per the plan; v1 forwards all). Unlike push.zig::pushMetrics this
/// does NOT use client.fetch: it pre-connects so it can put a send/recv deadline
/// on the socket (see forward_timeout_s) before driving the request, because the
/// forward is inline on the accept loop.
pub const HostedForwarder = struct {
    server_url: []const u8,
    otlp_token: []const u8,

    pub fn forwarder(self: *HostedForwarder) Forwarder {
        return .{ .ctx = self, .forwardFn = forward };
    }

    fn forward(ctx: ?*anyopaque, arena: Allocator, entries: []const logs_mod.LogEntry) anyerror!void {
        const self: *HostedForwarder = @ptrCast(@alignCast(ctx.?));
        if (entries.len == 0) return;

        const body = try buildForwardBody(arena, entries);
        // Trailing-slash trim mirrors push.zig::buildIngestUrl.
        const trimmed = std.mem.trimRight(u8, self.server_url, "/");
        const url = try std.fmt.allocPrint(arena, "{s}/v1/logs", .{trimmed});
        const auth = try std.fmt.allocPrint(arena, "Bearer {s}", .{self.otlp_token});

        // The client lives per-request (allocations die with the arena); no
        // connection reuse across requests, matching push.zig's per-push client.
        var client = std.http.Client{ .allocator = arena };
        defer client.deinit();

        const uri = try std.Uri.parse(url);
        const protocol = std.http.Client.Protocol.fromUri(uri) orelse return error.UnsupportedUriScheme;
        var host_buf: [std.Uri.host_name_max]u8 = undefined;
        const host = try uri.getHost(&host_buf);
        // Client.Protocol.port is private; the default port per scheme is fixed.
        const port = uri.port orelse switch (protocol) {
            .plain => @as(u16, 80),
            .tls => @as(u16, 443),
        };

        // Pre-connect so we can set the deadline on the socket BEFORE any
        // request byte moves. keep_alive=false on the request below tears this
        // connection down after the one POST (no pooling across requests).
        const connection = try client.connect(host, port, protocol);
        const deadline = std.posix.timeval{ .sec = forward_timeout_s, .usec = 0 };
        const fd = connection.stream_reader.getStream().handle;
        inline for (.{ std.posix.SO.SNDTIMEO, std.posix.SO.RCVTIMEO }) |opt| {
            std.posix.setsockopt(fd, std.posix.SOL.SOCKET, opt, std.mem.asBytes(&deadline)) catch |err| {
                // Without the deadline a hung forward could wedge the accept
                // loop; refuse to forward rather than risk that. The caller
                // turns this into a 503 so the SDK retries.
                std.log.warn("otlp receiver: set forward deadline failed: {}", .{err});
                return error.OtlpForwardDeadlineUnset;
            };
        }

        var req = try client.request(.POST, uri, .{
            .keep_alive = false,
            .connection = connection,
            .extra_headers = &.{
                .{ .name = "content-type", .value = "application/json" },
                .{ .name = "authorization", .value = auth },
            },
        });
        defer req.deinit();

        req.transfer_encoding = .{ .content_length = body.len };
        var body_writer = try req.sendBodyUnflushed(&.{});
        try body_writer.writer.writeAll(body);
        try body_writer.end();
        try req.connection.?.flush();

        var redirect_buffer: [1024]u8 = undefined;
        var response = try req.receiveHead(&redirect_buffer);
        // Drain the response body so the deadline also bounds a peer that sends
        // headers then stalls mid-body.
        _ = response.reader(&.{}).discardRemaining() catch {};

        if (response.head.status.class() != .success) {
            return error.OtlpForwardRejected;
        }
    }
};

/// Inverse of `syslogPriorityFromSeverity`: map a syslog priority back to an
/// OTLP severityNumber for the re-serialized forward body. Lossy where syslog
/// is coarser than OTLP (notice has no OTLP band; it forwards as INFO2 = 10,
/// which maps back to info).
pub fn otlpSeverityFromPriority(priority: u8) i64 {
    return switch (priority) {
        0, 1, 2 => 21, // emerg/alert/crit -> FATAL
        3 => 17, // err -> ERROR
        4 => 13, // warning -> WARN
        5 => 10, // notice -> INFO2
        6 => 9, // info -> INFO
        else => 5, // debug -> DEBUG
    };
}

/// Build the minimal OTLP/JSON logs envelope for the forward body. Entries are
/// grouped by identifier (the mapped service name) so each service lands under
/// its own resourceLogs with a `service.name` resource attribute - OTLP puts
/// service identity on the Resource, not the record. Everything is allocated
/// from `arena` (per-request lifetime).
fn buildForwardBody(arena: Allocator, entries: []const logs_mod.LogEntry) ![]u8 {
    const Group = struct {
        service_name: ?[]const u8,
        records: std.ArrayList(LogRecord),
    };

    // O(entries * distinct services); a request rarely carries more than a
    // handful of services, so a linear group scan beats a hash map here.
    var groups: std.ArrayList(Group) = .empty;
    outer: for (entries) |entry| {
        const record = LogRecord{
            .timeUnixNano = try std.fmt.allocPrint(
                arena,
                "{d}",
                .{@as(i128, entry.timestamp) * std.time.ns_per_s},
            ),
            .severityNumber = otlpSeverityFromPriority(entry.priority),
            .body = .{ .stringValue = entry.message },
        };
        for (groups.items) |*group| {
            const same = if (group.service_name) |name|
                if (entry.identifier) |id| std.mem.eql(u8, name, id) else false
            else
                entry.identifier == null;
            if (same) {
                try group.records.append(arena, record);
                continue :outer;
            }
        }
        var group = Group{ .service_name = entry.identifier, .records = .empty };
        try group.records.append(arena, record);
        try groups.append(arena, group);
    }

    const resource_logs = try arena.alloc(ResourceLogs, groups.items.len);
    for (resource_logs, groups.items) |*rl, group| {
        const scope_logs = try arena.alloc(ScopeLogs, 1);
        scope_logs[0] = .{ .logRecords = group.records.items };
        var resource: ?Resource = null;
        if (group.service_name) |name| {
            const attributes = try arena.alloc(KeyValue, 1);
            attributes[0] = .{ .key = "service.name", .value = .{ .stringValue = name } };
            resource = .{ .attributes = attributes };
        }
        rl.* = .{ .resource = resource, .scopeLogs = scope_logs };
    }

    const envelope = LogsData{ .resourceLogs = resource_logs };
    return std.fmt.allocPrint(arena, "{f}", .{std.json.fmt(envelope, .{
        .emit_null_optional_fields = false,
    })});
}

/// The localhost OTLP/HTTP receiver. `init` binds (so the bound port is
/// readable before any thread exists - tests bind port 0 and read it back),
/// `start` spawns the accept-loop thread, `stop` unblocks the loop and joins.
///
/// SHUTDOWN MECHANISM: posix.accept maps EBADF to unreachable ("always a race
/// condition"), so close()-ing the listener fd from another thread to unblock
/// accept() is explicitly unsafe. Instead `stop` calls shutdown() on the
/// listening socket: on Linux that wakes the blocked accept() with EINVAL,
/// which std.posix surfaces as error.SocketNotListening (verified empirically
/// on this stdlib + kernel; encoded as the loop's exit condition). The fd is
/// only closed after the thread has joined.
pub const Receiver = struct {
    allocator: Allocator,
    // Two loopback listeners: 127.0.0.1 AND ::1. An SDK pointed at
    // "localhost:4318" resolves localhost via getaddrinfo, which commonly yields
    // ::1 BEFORE 127.0.0.1; a v4-only listener would then refuse those clients.
    // We bind BOTH loopback addresses on the same port. We do NOT bind a
    // dual-stack `::` (that is all-interfaces, violating the loopback-only
    // project rule) - separate v4 + v6 loopback sockets keep the receiver
    // strictly on-host. The v6 listener is optional: a host with IPv6 disabled
    // fails that bind, and we run v4-only rather than refuse to start.
    listener: std.net.Server,
    listener6: ?std.net.Server,
    running: *std.atomic.Value(bool),
    forwarder: Forwarder,
    thread: ?std.Thread = null,

    pub fn init(
        allocator: Allocator,
        port: u16,
        running: *std.atomic.Value(bool),
        forwarder: Forwarder,
    ) !Receiver {
        // 127.0.0.1 first - the primary loopback listener, and the one whose
        // bound port we report. reuse_address so a daemon restart doesn't fail
        // the bind on a lingering TIME_WAIT socket from the previous run.
        const address = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, port);
        const listener = try address.listen(.{ .reuse_address = true });
        errdefer {
            var l = listener;
            l.deinit();
        }

        // ::1 on the SAME bound port (resolve the v4 port first so a port-0 bind
        // gives both listeners one shared ephemeral port; v4 and v6 are distinct
        // address families so the port never collides between them). A v6 bind
        // failure (IPv6 disabled) is non-fatal: log and run v4-only.
        const bound_port = listener.listen_address.getPort();
        const address6 = std.net.Address.initIp6(
            .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 },
            bound_port,
            0,
            0,
        );
        const listener6: ?std.net.Server = address6.listen(.{ .reuse_address = true }) catch |err| blk: {
            std.log.warn("otlp receiver: ::1 bind failed ({}); serving on 127.0.0.1 only", .{err});
            break :blk null;
        };

        return .{
            .allocator = allocator,
            .listener = listener,
            .listener6 = listener6,
            .running = running,
            .forwarder = forwarder,
        };
    }

    /// The actual bound port - differs from the requested one when binding
    /// port 0 (tests use this to avoid fixed-port collisions). Both loopback
    /// listeners share this port.
    pub fn boundPort(self: *const Receiver) u16 {
        return self.listener.listen_address.getPort();
    }

    /// Spawn the accept-loop thread. Separate from `init` so the thread only
    /// ever captures the Receiver's FINAL address - call this after the struct
    /// has been moved to where it will live.
    pub fn start(self: *Receiver) !void {
        self.thread = try std.Thread.spawn(.{}, run, .{self});
    }

    /// Unblock the accept loop, join the thread, close both listeners. Safe to
    /// call whether or not `start` succeeded. An in-flight request finishes
    /// first (bounded by `read_timeout_s` against a hung client), so join can
    /// take up to that long in the worst case. shutdown() on each listener wakes
    /// a blocked accept/poll; the fds are only closed after the thread joins.
    pub fn stop(self: *Receiver) void {
        std.posix.shutdown(self.listener.stream.handle, .both) catch {};
        if (self.listener6) |l6| std.posix.shutdown(l6.stream.handle, .both) catch {};
        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
        self.listener.deinit();
        if (self.listener6) |*l6| l6.deinit();
    }

    fn run(self: *Receiver) void {
        // poll() both loopback listeners so one synchronous loop services
        // whichever family a client connected on. With only the v4 listener this
        // degenerates to a single-fd poll. stop()'s shutdown() makes poll report
        // the fd readable, the subsequent accept() returns SocketNotListening,
        // and the loop exits - same wake path as the prior bare-accept loop.
        while (self.running.load(.seq_cst)) {
            var fds: [2]std.posix.pollfd = undefined;
            var n: usize = 0;
            fds[n] = .{ .fd = self.listener.stream.handle, .events = std.posix.POLL.IN, .revents = 0 };
            const v4_idx = n;
            n += 1;
            const v6_idx: ?usize = if (self.listener6) |l6| blk: {
                fds[n] = .{ .fd = l6.stream.handle, .events = std.posix.POLL.IN, .revents = 0 };
                const idx = n;
                n += 1;
                break :blk idx;
            } else null;

            _ = std.posix.poll(fds[0..n], -1) catch |err| {
                std.log.warn("otlp receiver: poll failed: {}", .{err});
                std.Thread.sleep(100 * std.time.ns_per_ms);
                continue;
            };

            // Accept from each ready listener. POLL.IN covers a pending
            // connection; POLL.HUP/POLL.ERR/POLL.NVAL fire on stop()'s shutdown,
            // and the accept() below then returns SocketNotListening to exit.
            const ready_mask = std.posix.POLL.IN | std.posix.POLL.HUP | std.posix.POLL.ERR | std.posix.POLL.NVAL;
            if ((fds[v4_idx].revents & ready_mask) != 0) {
                if (!self.acceptOne(&self.listener)) return;
            }
            if (v6_idx) |idx| {
                if ((fds[idx].revents & ready_mask) != 0) {
                    if (self.listener6) |*l6| {
                        if (!self.acceptOne(l6)) return;
                    }
                }
            }
        }
    }

    /// Accept and serve ONE connection from `listener`. Returns false when the
    /// loop must EXIT (stop()'s shutdown surfaced as SocketNotListening), true
    /// to keep looping. A transient accept error backs off and keeps looping.
    fn acceptOne(self: *Receiver, listener: *std.net.Server) bool {
        const conn = listener.accept() catch |err| switch (err) {
            // stop()'s shutdown() surfaces here; signal the loop to exit
            // unconditionally so stop() always joins promptly.
            error.SocketNotListening => return false,
            // poll said readable but accept couldn't complete (e.g. the peer
            // reset between poll and accept) - skip this one, keep looping.
            error.WouldBlock, error.ConnectionAborted, error.ConnectionResetByPeer => return true,
            else => {
                std.log.warn("otlp receiver: accept failed: {}", .{err});
                // Back off so a persistent accept failure (fd exhaustion)
                // doesn't spin the loop hot.
                std.Thread.sleep(100 * std.time.ns_per_ms);
                return true;
            },
        };
        defer conn.stream.close();
        self.serveConnection(conn.stream);
        return true;
    }

    /// Service ONE request on the accepted connection, then close it. One
    /// request per connection (no keep-alive) naturally bounds how long a
    /// single producer can hold the single-threaded loop; SDKs reconnect.
    fn serveConnection(self: *Receiver, stream: std.net.Stream) void {
        const timeout = std.posix.timeval{ .sec = read_timeout_s, .usec = 0 };
        std.posix.setsockopt(
            stream.handle,
            std.posix.SOL.SOCKET,
            std.posix.SO.RCVTIMEO,
            std.mem.asBytes(&timeout),
        ) catch |err| {
            // Without the deadline a hung client could wedge the loop forever;
            // refuse to serve rather than risk that.
            std.log.warn("otlp receiver: set read deadline failed: {}", .{err});
            return;
        };

        // recv_buffer must hold the entire request head (receiveHead errors
        // with HttpHeadersOversize otherwise); 8 KiB matches common proxy/
        // server header ceilings.
        var recv_buffer: [8192]u8 = undefined;
        var send_buffer: [1024]u8 = undefined;
        var reader = stream.reader(&recv_buffer);
        var writer = stream.writer(&send_buffer);
        var http_server = std.http.Server.init(reader.interface(), &writer.interface);

        // A malformed/empty head (or a read deadline firing) leaves nothing
        // useful to respond to; just drop the connection.
        var request = http_server.receiveHead() catch return;
        self.handleRequest(&request) catch |err| {
            std.log.warn("otlp receiver: request handling failed: {}", .{err});
        };
    }

    fn handleRequest(self: *Receiver, request: *std.http.Server.Request) !void {
        // Every response sets keep_alive = false (one request per connection),
        // which also means respond() never tries to drain an unread body.
        if (request.head.method != .POST) {
            return request.respond("", .{ .status = .method_not_allowed, .keep_alive = false });
        }
        if (!std.mem.eql(u8, request.head.target, "/v1/logs")) {
            return request.respond("", .{ .status = .not_found, .keep_alive = false });
        }
        // JSON is the only OTLP encoding the daemon decodes (no protobuf
        // runtime without a dependency - plan 26 phase 6). An explicit
        // non-JSON content-type gets the OTLP-spec 415 so an SDK left on its
        // protobuf default fails legibly instead of as "malformed JSON".
        if (request.head.content_type) |content_type| {
            if (!std.ascii.startsWithIgnoreCase(content_type, "application/json")) {
                return request.respond("", .{ .status = .unsupported_media_type, .keep_alive = false });
            }
        }
        // Reject oversize declarations before reading a single body byte.
        if (request.head.content_length) |content_length| {
            if (content_length > max_body_bytes) {
                return request.respond("", .{ .status = .payload_too_large, .keep_alive = false });
            }
        }

        // MEMORY OWNERSHIP: everything this request allocates - the body, the
        // parsed envelope, the mapped entries, the re-serialized forward body -
        // comes from this arena and is freed when the request ends, so the
        // receiver runs indefinitely without growth.
        var arena_state = std.heap.ArenaAllocator.init(self.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        var body_buffer: [4096]u8 = undefined;
        const body_reader = try request.readerExpectContinue(&body_buffer);
        // limited(max + 1): allocRemaining returns StreamTooLong once the limit
        // is REACHED, so the +1 admits a body of exactly max_body_bytes. This
        // also catches chunked bodies and lying content-length headers that
        // slipped past the declared-length check above.
        const body = body_reader.allocRemaining(arena, .limited(max_body_bytes + 1)) catch |err| switch (err) {
            error.StreamTooLong => {
                return request.respond("", .{ .status = .payload_too_large, .keep_alive = false });
            },
            else => return err,
        };

        const logs_data = std.json.parseFromSliceLeaky(LogsData, arena, body, .{
            .ignore_unknown_fields = true,
        }) catch {
            return request.respond("", .{ .status = .bad_request, .keep_alive = false });
        };

        var entries: std.ArrayList(logs_mod.LogEntry) = .empty;
        const now = std.time.timestamp();
        for (logs_data.resourceLogs) |resource_logs| {
            const service_name = serviceName(resource_logs);
            for (resource_logs.scopeLogs) |scope_logs| {
                for (scope_logs.logRecords) |record| {
                    try entries.append(arena, try mapLogRecord(arena, record, service_name, now));
                }
            }
        }

        // v1 forwards ALL records (the sampling knob is a later phase) and the
        // forward runs INLINE before the local response. Phase 2 has NO on-host
        // persistence (phase 3, deliberately gated), so a forward we cannot
        // complete is data we cannot keep: answering 200 would tell the SDK we
        // own bytes we just dropped, and the SDK would never retry. So a forward
        // failure - whether the hosted POST was rejected/unreachable or
        // forwarding isn't configured at all (drop_forwarder) - surfaces as 503,
        // the OTLP-spec retryable status, so the SDK holds and re-sends instead.
        // Phase 3 persistence is what will let an accepted record survive a
        // forward failure and turn this back into an unconditional 200.
        self.forwarder.forward(arena, entries.items) catch |err| {
            std.log.warn("otlp receiver: forward to hosted /v1/logs failed: {} - 503 so the SDK retries", .{err});
            return request.respond("", .{ .status = .service_unavailable, .keep_alive = false });
        };

        // OTLP/HTTP success: 200 with the encoded empty ExportLogsServiceResponse.
        try request.respond("{}", .{
            .status = .ok,
            .keep_alive = false,
            .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }},
        });
    }
};

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
    // Fits in i128 but the seconds overflow i64: must fall back, not panic.
    try std.testing.expectEqual(@as(i64, 42), unixSecondsFromNano("10000000000000000000000000000", 42));
    try std.testing.expectEqual(@as(i64, 42), unixSecondsFromNano("-10000000000000000000000000000", 42));
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

test "mapLogRecord renders non-string AnyValue bodies instead of emptying them" {
    const allocator = std.testing.allocator;

    const Case = struct { body: AnyValue, want: []const u8 };
    const cases = [_]Case{
        .{ .body = .{ .intValue = "42" }, .want = "42" }, // OTLP/JSON int is a string
        .{ .body = .{ .doubleValue = 1.5 }, .want = "1.5" },
        .{ .body = .{ .boolValue = true }, .want = "true" },
        .{ .body = .{ .boolValue = false }, .want = "false" },
        // Complex/binary bodies must not silently empty: a marked placeholder.
        .{ .body = .{ .bytesValue = "aGk=" }, .want = "[otlp bytesValue]" },
        .{ .body = .{ .arrayValue = .{} }, .want = "[otlp arrayValue]" },
        .{ .body = .{ .kvlistValue = .{} }, .want = "[otlp kvlistValue]" },
    };
    for (cases) |c| {
        var entry = try mapLogRecord(allocator, .{ .body = c.body }, null, 0);
        defer entry.deinit(allocator);
        try std.testing.expectEqualStrings(c.want, entry.message);
    }
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

test "otlpSeverityFromPriority round-trips through syslogPriorityFromSeverity" {
    // The push-side severity floor (always_push_priority) keys off syslog
    // priority, so the forward body's severityNumber must land back on the
    // same priority when the hosted side applies the inverse mapping.
    for ([_]u8{ 2, 3, 4, 6, 7 }) |priority| {
        try std.testing.expectEqual(priority, syslogPriorityFromSeverity(otlpSeverityFromPriority(priority)));
    }
    // Lossy edges: emerg/alert coarsen to crit, notice to info.
    try std.testing.expectEqual(@as(u8, 2), syslogPriorityFromSeverity(otlpSeverityFromPriority(0)));
    try std.testing.expectEqual(@as(u8, 6), syslogPriorityFromSeverity(otlpSeverityFromPriority(5)));
}

test "buildForwardBody groups records by service and re-serializes OTLP/JSON" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const entries = [_]logs_mod.LogEntry{
        .{ .timestamp = 1_739_443_200, .source = "otlp", .unit = "checkout-api", .identifier = "checkout-api", .systemd_unit = "checkout-api", .priority = 3, .message = "request failed", .pid = null },
        .{ .timestamp = 1_739_443_201, .source = "otlp", .unit = null, .identifier = null, .systemd_unit = null, .priority = 6, .message = "anonymous record", .pid = null },
        .{ .timestamp = 1_739_443_202, .source = "otlp", .unit = "checkout-api", .identifier = "checkout-api", .systemd_unit = "checkout-api", .priority = 6, .message = "request handled", .pid = null },
    };

    const body = try buildForwardBody(arena, &entries);

    // The forward body must itself parse as the envelope we accept on receive.
    const round = try std.json.parseFromSliceLeaky(LogsData, arena, body, .{
        .ignore_unknown_fields = true,
    });

    // Two distinct services (checkout-api, none) -> two resourceLogs groups.
    try std.testing.expectEqual(@as(usize, 2), round.resourceLogs.len);

    const named = round.resourceLogs[0];
    try std.testing.expectEqualStrings("checkout-api", serviceName(named).?);
    const named_records = named.scopeLogs[0].logRecords;
    try std.testing.expectEqual(@as(usize, 2), named_records.len);
    try std.testing.expectEqualStrings("1739443200000000000", named_records[0].timeUnixNano.?);
    try std.testing.expectEqual(@as(i64, 17), named_records[0].severityNumber.?); // err -> ERROR
    try std.testing.expectEqualStrings("request failed", named_records[0].body.?.stringValue.?);
    try std.testing.expectEqualStrings("request handled", named_records[1].body.?.stringValue.?);

    const anonymous = round.resourceLogs[1];
    try std.testing.expect(serviceName(anonymous) == null);
    try std.testing.expectEqualStrings("anonymous record", anonymous.scopeLogs[0].logRecords[0].body.?.stringValue.?);
}

// ── Receiver socket tests ──
// The first real-socket tests in this codebase. Determinism rules: bind port 0
// and read the bound port back (no fixed ports to collide on), synchronize on
// the request/response ordering (forward() runs before the 200 is written, so
// once a fetch returns the fake forwarder's state is complete) and on stop()'s
// join - never on sleeps.

/// Fake forward seam: records the mapped entries it was handed. Entries live
/// in the request arena and die with the request, so the fake dupes what the
/// test asserts on with its own allocator.
const TestForwarder = struct {
    allocator: Allocator,
    mutex: std.Thread.Mutex = .{},
    messages: std.ArrayList([]u8) = .empty,
    identifiers: std.ArrayList(?[]u8) = .empty,
    fail: bool = false,

    fn forward(ctx: ?*anyopaque, _: Allocator, entries: []const logs_mod.LogEntry) anyerror!void {
        const self: *TestForwarder = @ptrCast(@alignCast(ctx.?));
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.fail) return error.OtlpForwardRejected;
        for (entries) |entry| {
            try self.messages.append(self.allocator, try self.allocator.dupe(u8, entry.message));
            const id: ?[]u8 = if (entry.identifier) |v| try self.allocator.dupe(u8, v) else null;
            try self.identifiers.append(self.allocator, id);
        }
    }

    fn forwarder(self: *TestForwarder) Forwarder {
        return .{ .ctx = self, .forwardFn = forward };
    }

    fn deinit(self: *TestForwarder) void {
        for (self.messages.items) |m| self.allocator.free(m);
        self.messages.deinit(self.allocator);
        for (self.identifiers.items) |id| if (id) |v| self.allocator.free(v);
        self.identifiers.deinit(self.allocator);
    }
};

fn testFetch(
    allocator: Allocator,
    port: u16,
    method: std.http.Method,
    path: []const u8,
    payload: ?[]const u8,
    content_type: []const u8,
) !std.http.Status {
    const url = try std.fmt.allocPrint(allocator, "http://127.0.0.1:{d}{s}", .{ port, path });
    defer allocator.free(url);

    var client = std.http.Client{ .allocator = allocator };
    defer client.deinit();

    const result = try client.fetch(.{
        .location = .{ .url = url },
        .method = method,
        .payload = payload,
        .extra_headers = &.{.{ .name = "content-type", .value = content_type }},
    });
    return result.status;
}

test "receiver stop() unblocks the blocked accept and joins within a deadline" {
    var running = std.atomic.Value(bool).init(true);
    var receiver = try Receiver.init(std.testing.allocator, 0, &running, drop_forwarder);
    try receiver.start();

    running.store(false, .seq_cst);
    const before_ms = std.time.milliTimestamp();
    receiver.stop(); // must shutdown-wake the accept; a plain close would not be safe
    const elapsed_ms = std.time.milliTimestamp() - before_ms;
    try std.testing.expect(elapsed_ms < 5_000);
}

test "receiver round-trips a real OTLP/JSON POST to the forward seam" {
    const allocator = std.testing.allocator;

    var fake = TestForwarder{ .allocator = allocator };
    defer fake.deinit();

    var running = std.atomic.Value(bool).init(true);
    var receiver = try Receiver.init(allocator, 0, &running, fake.forwarder());
    try receiver.start();
    defer {
        running.store(false, .seq_cst);
        receiver.stop();
    }

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

    const url = try std.fmt.allocPrint(allocator, "http://127.0.0.1:{d}/v1/logs", .{receiver.boundPort()});
    defer allocator.free(url);

    var client = std.http.Client{ .allocator = allocator };
    defer client.deinit();

    var response_body: std.Io.Writer.Allocating = .init(allocator);
    defer response_body.deinit();

    const result = try client.fetch(.{
        .location = .{ .url = url },
        .method = .POST,
        .payload = body,
        .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }},
        .response_writer = &response_body.writer,
    });

    try std.testing.expectEqual(std.http.Status.ok, result.status);
    // OTLP empty-success body.
    try std.testing.expectEqualStrings("{}", response_body.writer.buffer[0..response_body.writer.end]);

    // forward() ran before the 200 was written, so the fake is already settled.
    fake.mutex.lock();
    defer fake.mutex.unlock();
    try std.testing.expectEqual(@as(usize, 2), fake.messages.items.len);
    try std.testing.expectEqualStrings("request handled", fake.messages.items[0]);
    try std.testing.expectEqualStrings("request failed", fake.messages.items[1]);
    try std.testing.expectEqualStrings("checkout-api", fake.identifiers.items[0].?);
    try std.testing.expectEqualStrings("checkout-api", fake.identifiers.items[1].?);
}

test "receiver also serves the request over the ::1 loopback" {
    const allocator = std.testing.allocator;

    var fake = TestForwarder{ .allocator = allocator };
    defer fake.deinit();

    var running = std.atomic.Value(bool).init(true);
    var receiver = try Receiver.init(allocator, 0, &running, fake.forwarder());
    try receiver.start();
    defer {
        running.store(false, .seq_cst);
        receiver.stop();
    }

    // Skip on a host with IPv6 loopback disabled (the v6 bind is non-fatal and
    // left null in that case) - there is nothing to round-trip.
    const listener6 = receiver.listener6 orelse return error.SkipZigTest;

    // Raw TCP to the ::1 listen address: std.http.Client's URI path does not
    // accept a bracketed IPv6 literal on this stdlib, and a raw request proves
    // the v6 socket accepts and serves directly. One request per connection, so
    // a hand-written HTTP/1.1 head + body and a single status-line read suffice.
    const stream = try std.net.tcpConnectToAddress(listener6.listen_address);
    defer stream.close();

    const body =
        \\{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"v6 hi"}}]}]}]}
    ;
    const request = try std.fmt.allocPrint(
        allocator,
        "POST /v1/logs HTTP/1.1\r\nhost: ::1\r\ncontent-type: application/json\r\ncontent-length: {d}\r\nconnection: close\r\n\r\n{s}",
        .{ body.len, body },
    );
    defer allocator.free(request);
    try stream.writeAll(request);

    var response: std.ArrayList(u8) = .empty;
    defer response.deinit(allocator);
    var read_buffer: [1024]u8 = undefined;
    while (true) {
        const n = stream.read(&read_buffer) catch break;
        if (n == 0) break;
        try response.appendSlice(allocator, read_buffer[0..n]);
    }
    try std.testing.expect(std.mem.startsWith(u8, response.items, "HTTP/1.1 200"));

    fake.mutex.lock();
    defer fake.mutex.unlock();
    try std.testing.expectEqual(@as(usize, 1), fake.messages.items.len);
    try std.testing.expectEqualStrings("v6 hi", fake.messages.items[0]);
}

test "receiver 503s the producer when the forward fails" {
    const allocator = std.testing.allocator;

    var fake = TestForwarder{ .allocator = allocator, .fail = true };
    defer fake.deinit();

    var running = std.atomic.Value(bool).init(true);
    var receiver = try Receiver.init(allocator, 0, &running, fake.forwarder());
    try receiver.start();
    defer {
        running.store(false, .seq_cst);
        receiver.stop();
    }

    const body =
        \\{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"hi"}}]}]}]}
    ;
    const status = try testFetch(allocator, receiver.boundPort(), .POST, "/v1/logs", body, "application/json");
    // No persistence in phase 2: a record we accepted but could not forward is
    // gone. 503 (retryable) keeps it in the SDK's buffer instead of dropping it.
    try std.testing.expectEqual(std.http.Status.service_unavailable, status);
}

test "receiver 503s when forwarding is not configured (drop_forwarder)" {
    const allocator = std.testing.allocator;

    var running = std.atomic.Value(bool).init(true);
    var receiver = try Receiver.init(allocator, 0, &running, drop_forwarder);
    try receiver.start();
    defer {
        running.store(false, .seq_cst);
        receiver.stop();
    }

    const body =
        \\{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"hi"}}]}]}]}
    ;
    // receiver_enabled without forwarding wired up can do nothing useful with a
    // log in phase 2; 503 tells the operator the receiver is unwired rather than
    // silently accepting-and-dropping.
    const status = try testFetch(allocator, receiver.boundPort(), .POST, "/v1/logs", body, "application/json");
    try std.testing.expectEqual(std.http.Status.service_unavailable, status);
}

test "receiver survives an oversize timeUnixNano on the receive path" {
    const allocator = std.testing.allocator;

    var fake = TestForwarder{ .allocator = allocator };
    defer fake.deinit();

    var running = std.atomic.Value(bool).init(true);
    var receiver = try Receiver.init(allocator, 0, &running, fake.forwarder());
    try receiver.start();
    defer {
        running.store(false, .seq_cst);
        receiver.stop();
    }

    // Seconds beyond i64 range, but parseable as i128 - the case the cast in
    // unixSecondsFromNano must degrade on. A regression panics the receiver
    // thread (aborting this test binary), so the assertions below double as
    // proof the request completed.
    const body =
        \\{"resourceLogs":[{"scopeLogs":[{"logRecords":[
        \\  {"timeUnixNano":"10000000000000000000000000000",
        \\   "body":{"stringValue":"bad clock"}}]}]}]}
    ;
    const status = try testFetch(allocator, receiver.boundPort(), .POST, "/v1/logs", body, "application/json");
    try std.testing.expectEqual(std.http.Status.ok, status);

    // The record still flows to the forward seam with its fallback timestamp.
    fake.mutex.lock();
    defer fake.mutex.unlock();
    try std.testing.expectEqual(@as(usize, 1), fake.messages.items.len);
    try std.testing.expectEqualStrings("bad clock", fake.messages.items[0]);
}

test "receiver rejects bad method, path, body, and content type" {
    const allocator = std.testing.allocator;

    var fake = TestForwarder{ .allocator = allocator };
    defer fake.deinit();

    var running = std.atomic.Value(bool).init(true);
    var receiver = try Receiver.init(allocator, 0, &running, fake.forwarder());
    try receiver.start();
    defer {
        running.store(false, .seq_cst);
        receiver.stop();
    }
    const port = receiver.boundPort();

    // Non-POST -> 405.
    try std.testing.expectEqual(
        std.http.Status.method_not_allowed,
        try testFetch(allocator, port, .GET, "/v1/logs", null, "application/json"),
    );
    // Unknown path -> 404 (metrics/traces are later phases).
    try std.testing.expectEqual(
        std.http.Status.not_found,
        try testFetch(allocator, port, .POST, "/v1/metrics", "{}", "application/json"),
    );
    // Malformed JSON -> 400.
    try std.testing.expectEqual(
        std.http.Status.bad_request,
        try testFetch(allocator, port, .POST, "/v1/logs", "{not json", "application/json"),
    );
    // OTLP/protobuf (the SDK wire default) -> 415, not a confusing 400.
    try std.testing.expectEqual(
        std.http.Status.unsupported_media_type,
        try testFetch(allocator, port, .POST, "/v1/logs", "\x0a\x00", "application/x-protobuf"),
    );

    // Nothing should have reached the forward seam.
    fake.mutex.lock();
    defer fake.mutex.unlock();
    try std.testing.expectEqual(@as(usize, 0), fake.messages.items.len);
}

test "receiver 413s a body declared over the cap without reading it" {
    const allocator = std.testing.allocator;

    var running = std.atomic.Value(bool).init(true);
    var receiver = try Receiver.init(allocator, 0, &running, drop_forwarder);
    try receiver.start();
    defer {
        running.store(false, .seq_cst);
        receiver.stop();
    }

    // Raw socket instead of std.http.Client: the server 413s from the declared
    // content-length BEFORE any body byte arrives, then closes. A client
    // mid-way through uploading 1 MiB would race that close (broken pipe vs
    // response); sending only the head and reading the reply is deterministic.
    const stream = try std.net.tcpConnectToAddress(receiver.listener.listen_address);
    defer stream.close();

    const head = try std.fmt.allocPrint(
        allocator,
        "POST /v1/logs HTTP/1.1\r\nhost: 127.0.0.1\r\ncontent-type: application/json\r\ncontent-length: {d}\r\n\r\n",
        .{max_body_bytes + 1},
    );
    defer allocator.free(head);
    try stream.writeAll(head);

    // Read until the server closes; the status line arrives first.
    var response: std.ArrayList(u8) = .empty;
    defer response.deinit(allocator);
    var read_buffer: [1024]u8 = undefined;
    while (true) {
        const n = stream.read(&read_buffer) catch break;
        if (n == 0) break;
        try response.appendSlice(allocator, read_buffer[0..n]);
    }
    try std.testing.expect(std.mem.startsWith(u8, response.items, "HTTP/1.1 413"));
}
