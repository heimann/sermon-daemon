//! Verified Zig HTTPS in a fixed child mode: the process timer covers DNS,
//! connect, TLS, writes and reads, including a peer that never makes progress.
//! No curl library/sysroot, shell, redirects or credentials in argv/files.
const std = @import("std");
const protocol = @import("host_log_protocol");
const c = @cImport({
    @cInclude("sys/time.h");
});
pub const Response = struct { status: u16, body: []const u8, server_time: ?i64 = null };
const Input = struct { origin: []const u8, key: []const u8, path: []const u8, body: []const u8, cap: usize };

fn validate(in: Input) !void {
    const uri = try std.Uri.parse(in.origin);
    if (!std.mem.eql(u8, uri.scheme, "https") or uri.host == null or uri.user != null or uri.password != null or uri.query != null or uri.fragment != null) return error.InvalidOrigin;
    if (in.cap > 8192 or in.body.len > 2 * 1024 * 1024 or in.origin.len > 4096 or in.key.len > 4096 or in.path.len > 256 or
        !std.mem.startsWith(u8, in.path, "/") or std.mem.indexOfAny(u8, in.key, "\r\n") != null) return error.InvalidRequest;
}

pub fn post(a: std.mem.Allocator, origin: []const u8, key: []const u8, path: []const u8, body: []const u8, cap: usize) !Response {
    const in = Input{ .origin = origin, .key = key, .path = path, .body = body, .cap = cap };
    try validate(in);
    const input = try std.json.Stringify.valueAlloc(a, in, .{});
    defer a.free(input);
    const exe = try std.fs.selfExePathAlloc(a);
    defer a.free(exe);
    var child = std.process.Child.init(&.{ exe, "--https-worker" }, a);
    child.stdin_behavior = .Pipe;
    child.stdout_behavior = .Pipe;
    child.stderr_behavior = .Ignore;
    try child.spawn();
    errdefer _ = child.kill() catch {};
    // Zig startup ignores SIGPIPE, so child timeout/disconnect is an error,
    // never a signal that kills the collection process.
    try child.stdin.?.writeAll(input);
    child.stdin.?.close();
    child.stdin = null;
    const output = try child.stdout.?.readToEndAlloc(a, 64 * 1024);
    defer a.free(output);
    const term = try child.wait();
    if (term != .Exited or term.Exited != 0) return error.HttpFailed;
    const parsed = try std.json.parseFromSlice(Response, a, output, .{});
    defer parsed.deinit();
    return .{ .status = parsed.value.status, .body = try a.dupe(u8, parsed.value.body), .server_time = parsed.value.server_time };
}

/// Private pipe protocol. The fixed executable has no inbound listener. Its
/// input is bounded even when invoked directly; only local parent supplies it.
pub fn childMain() !void {
    const timer = c.itimerval{ .it_interval = .{ .tv_sec = 0, .tv_usec = 0 }, .it_value = .{ .tv_sec = 5, .tv_usec = 0 } };
    if (c.setitimer(c.ITIMER_REAL, &timer, null) != 0) return error.TimerFailed;
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // JSON can expand every input byte to six bytes; source body <=2 MiB.
    const input = try (std.fs.File{ .handle = std.posix.STDIN_FILENO }).readToEndAlloc(a, 13 * 1024 * 1024);
    const parsed = try std.json.parseFromSlice(Input, a, input, .{});
    const in = parsed.value;
    try validate(in);
    const url = try std.fmt.allocPrint(a, "{s}{s}", .{ std.mem.trimRight(u8, in.origin, "/"), in.path });
    var client = std.http.Client{ .allocator = a };
    defer client.deinit();
    if (std.posix.getenv("SSL_CERT_FILE")) |ca| {
        try client.ca_bundle.addCertsFromFilePath(a, std.fs.cwd(), ca);
        client.next_https_rescan_certs = false;
    }
    var req = try client.request(.POST, try std.Uri.parse(url), .{
        .redirect_behavior = .not_allowed,
        .keep_alive = false,
        .headers = .{ .accept_encoding = .{ .override = "identity" } },
        .extra_headers = &.{
            .{ .name = "content-type", .value = "application/json" },
            .{ .name = "x-sermon-ingestion-key", .value = in.key },
        },
    });
    defer req.deinit();
    req.transfer_encoding = .{ .content_length = in.body.len };
    var body = try req.sendBodyUnflushed(&.{});
    try body.writer.writeAll(in.body);
    try body.end();
    try req.connection.?.flush();
    var response = try req.receiveHead(&.{});
    if (response.head.content_encoding != .identity) return error.UnexpectedEncoding;
    var server_time: ?i64 = null;
    var headers = response.head.iterateHeaders();
    while (headers.next()) |header| {
        if (std.ascii.eqlIgnoreCase(header.name, "date")) server_time = parseDate(header.value) catch return error.InvalidDate;
    }
    var bytes: [8193]u8 = undefined;
    var transfer: [64]u8 = undefined;
    const n = try response.reader(&transfer).readSliceShort(bytes[0 .. in.cap + 1]);
    if (n > in.cap) return error.ResponseTooLarge;
    const output = try std.json.Stringify.valueAlloc(a, Response{ .status = @intFromEnum(response.head.status), .body = bytes[0..n], .server_time = server_time }, .{});
    try (std.fs.File{ .handle = std.posix.STDOUT_FILENO }).writeAll(output);
}

/// HTTP servers emit IMF-fixdate in GMT. Reject malformed dates rather than
/// using local timezone or silently defeating the worker's UTC skew check.
fn parseDate(value: []const u8) !i64 {
    if (value.len != 29 or !std.mem.eql(u8, value[3..5], ", ") or value[7] != ' ' or value[11] != ' ' or value[16] != ' ' or !std.mem.eql(u8, value[25..], " GMT")) return error.InvalidDate;
    const months = "JanFebMarAprMayJunJulAugSepOctNovDec";
    var month: usize = 1;
    while (month <= 12) : (month += 1) {
        if (std.mem.eql(u8, months[(month - 1) * 3 ..][0..3], value[8..11])) break;
    }
    if (month > 12) return error.InvalidDate;
    var buf: [20]u8 = undefined;
    const iso = try std.fmt.bufPrint(&buf, "{s}-{d:0>2}-{s}T{s}Z", .{ value[12..16], month, value[5..7], value[17..25] });
    return @divTrunc(try protocol.timestamp(iso), 1000000);
}

test "transport refuses plaintext and credential-bearing origins" {
    const a = std.testing.allocator;
    try std.testing.expectError(error.InvalidOrigin, post(a, "http://example.invalid", "fixture-key", "/claim", "{}", 8192));
    try std.testing.expectError(error.InvalidOrigin, post(a, "https://user@example.invalid", "fixture-key", "/claim", "{}", 8192));
    try std.testing.expectError(error.InvalidRequest, post(a, "https://example.invalid", "fixture\nheader", "/claim", "{}", 8192));
    try std.testing.expectError(error.InvalidRequest, post(a, "https://example.invalid", "fixture", "/claim", "{}", 8193));
}

test "HTTP date is UTC and calendar validated" {
    try std.testing.expectEqual(@as(i64, 784111777), try parseDate("Sun, 06 Nov 1994 08:49:37 GMT"));
    try std.testing.expectError(error.InvalidDate, parseDate("Sun, 06 Nov 1994 08:49:37 EST"));
    try std.testing.expectError(error.InvalidTimestamp, parseDate("Sun, 31 Nov 1994 08:49:37 GMT"));
}
