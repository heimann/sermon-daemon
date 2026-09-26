const std = @import("std");
const Allocator = std.mem.Allocator;
const ChildProcess = std.process.Child;

const max_journal_record_bytes = 256 * 1024;
const journal_read_chunk_bytes = 16 * 1024;
const max_journal_bytes_per_collection_cycle = 16 * 1024 * 1024;
const journal_gap_warning_interval_seconds = 60;

const JournalRecordBuffer = struct {
    bytes: std.ArrayList(u8) = .{},
    max_bytes: usize,
    discarding: bool = false,

    const Event = enum { record, oversized };

    fn init(allocator: Allocator, max_bytes: usize) !JournalRecordBuffer {
        var self = JournalRecordBuffer{ .max_bytes = max_bytes };
        errdefer self.bytes.deinit(allocator);
        // Allocate the exact ceiling once. Appends below can then use
        // appendAssumeCapacity, so an attacker-shaped record cannot make the
        // retained allocation grow beyond the documented limit.
        try self.bytes.ensureTotalCapacityPrecise(allocator, max_bytes);
        return self;
    }

    fn deinit(self: *JournalRecordBuffer, allocator: Allocator) void {
        self.bytes.deinit(allocator);
    }

    fn push(self: *JournalRecordBuffer, byte: u8) ?Event {
        if (byte == '\n') {
            if (self.discarding) {
                self.discarding = false;
                return .oversized;
            }
            return .record;
        }
        if (self.discarding) return null;
        if (self.bytes.items.len == self.max_bytes) {
            self.bytes.clearRetainingCapacity();
            self.discarding = true;
            return null;
        }
        self.bytes.appendAssumeCapacity(byte);
        return null;
    }

    fn clearRecord(self: *JournalRecordBuffer) void {
        std.debug.assert(!self.discarding);
        self.bytes.clearRetainingCapacity();
    }

    fn hasPartialRecord(self: JournalRecordBuffer) bool {
        return self.discarding or self.bytes.items.len != 0;
    }
};

const JournalScanResult = struct {
    bytes_consumed: usize,
    event: ?JournalRecordBuffer.Event,
};

fn scanJournalBytes(buffer: *JournalRecordBuffer, input: []const u8, max_bytes: usize) JournalScanResult {
    const limit = @min(input.len, max_bytes);
    for (input[0..limit], 0..) |byte, i| {
        if (buffer.push(byte)) |event| {
            return .{ .bytes_consumed = i + 1, .event = event };
        }
    }
    return .{ .bytes_consumed = limit, .event = null };
}

/// Safely read a string field from a parsed JSON value. journald `-o json`
/// renders any non-UTF8/binary field value as an ARRAY of byte integers, so a
/// field like MESSAGE can legitimately be a `.array` (or any other tag).
/// Accessing `.string` on the wrong union tag is illegal behavior that panics
/// in safe builds, so route every journald field read through this guard:
/// a missing or non-string field is treated the same as absent (null).
fn jsonStr(v: ?std.json.Value) ?[]const u8 {
    return switch (v orelse return null) {
        .string => |s| s,
        else => null,
    };
}

/// Log entry structure - consumed by storage layer
pub const LogEntry = struct {
    timestamp: i64, // Unix timestamp (seconds)
    source: []const u8, // "systemd" or file path
    unit: ?[]const u8, // Back-compat grouping key: identifier if present, otherwise systemd_unit
    identifier: ?[]const u8, // SYSLOG_IDENTIFIER for journald entries
    systemd_unit: ?[]const u8, // _SYSTEMD_UNIT for journald entries
    priority: u8, // syslog priority 0-7 (0=emerg, 7=debug)
    message: []const u8, // log message content
    pid: ?u32, // process ID if available
    trace_id: ?[]const u8 = null,

    pub fn deinit(self: *LogEntry, allocator: Allocator) void {
        if (self.trace_id) |trace| allocator.free(trace);
        allocator.free(self.source);
        if (self.unit) |unit| {
            allocator.free(unit);
        }
        if (self.identifier) |identifier| {
            allocator.free(identifier);
        }
        if (self.systemd_unit) |systemd_unit| {
            allocator.free(systemd_unit);
        }
        allocator.free(self.message);
    }
};

/// Log source type
pub const LogSource = union(enum) {
    systemd: void,
    file: []const u8,
};

/// Main log tailer interface
pub const LogTailer = struct {
    allocator: Allocator,
    sources: []LogSource,
    journal_tailer: ?*JournalTailer,
    journal_bytes_remaining: usize,
    file_tailers: std.ArrayList(*FileTailer),

    pub fn init(allocator: Allocator, sources: []const LogSource) !LogTailer {
        var journal_tailer: ?*JournalTailer = null;
        var file_tailers = std.ArrayList(*FileTailer){};
        errdefer {
            if (journal_tailer) |jt| {
                jt.deinit();
                allocator.destroy(jt);
            }
            for (file_tailers.items) |ft| {
                ft.deinit();
                allocator.destroy(ft);
            }
            file_tailers.deinit(allocator);
        }

        // Copy sources
        const sources_copy = try allocator.alloc(LogSource, sources.len);
        for (sources, 0..) |source, i| {
            sources_copy[i] = switch (source) {
                .systemd => .systemd,
                .file => |path| .{ .file = try allocator.dupe(u8, path) },
            };
        }

        // Initialize tailers for each source
        for (sources_copy) |source| {
            switch (source) {
                .systemd => {
                    if (journal_tailer == null) {
                        const jt = try allocator.create(JournalTailer);
                        jt.* = try JournalTailer.init(allocator);
                        journal_tailer = jt;
                    }
                },
                .file => |path| {
                    const ft = try allocator.create(FileTailer);
                    ft.* = try FileTailer.init(allocator, path);
                    try file_tailers.append(allocator, ft);
                },
            }
        }

        return LogTailer{
            .allocator = allocator,
            .sources = sources_copy,
            .journal_tailer = journal_tailer,
            .journal_bytes_remaining = max_journal_bytes_per_collection_cycle,
            .file_tailers = file_tailers,
        };
    }

    pub fn deinit(self: *LogTailer) void {
        if (self.journal_tailer) |jt| {
            jt.deinit();
            self.allocator.destroy(jt);
        }
        for (self.file_tailers.items) |ft| {
            ft.deinit();
            self.allocator.destroy(ft);
        }
        self.file_tailers.deinit(self.allocator);

        for (self.sources) |source| {
            switch (source) {
                .systemd => {},
                .file => |path| self.allocator.free(path),
            }
        }
        self.allocator.free(self.sources);
    }

    /// Reset the source-work allowance once at the start of a collection cycle.
    /// Valid entries and oversized/discarded bytes share this bound.
    pub fn beginCollectionCycle(self: *LogTailer) void {
        self.journal_bytes_remaining = max_journal_bytes_per_collection_cycle;
    }

    /// Get the next available log entry without blocking.
    pub fn next(self: *LogTailer) !?LogEntry {
        // Simple round-robin: check journal first, then files
        // In a production system, this would use select/poll for efficiency

        if (self.journal_tailer) |jt| journal: {
            if (self.journal_bytes_remaining == 0) break :journal;
            const poll = try jt.next(self.journal_bytes_remaining);
            std.debug.assert(poll.bytes_processed <= self.journal_bytes_remaining);
            self.journal_bytes_remaining -= poll.bytes_processed;
            if (poll.entry) |entry| {
                return entry;
            }
        }

        for (self.file_tailers.items) |ft| {
            if (try ft.next()) |entry| {
                return entry;
            }
        }

        return null; // No entries available right now
    }
};

/// Systemd journal tailer using journalctl subprocess
const JournalTailer = struct {
    allocator: Allocator,
    process: ChildProcess,
    record_buffer: JournalRecordBuffer,
    read_buffer: [journal_read_chunk_bytes]u8 = undefined,
    read_start: usize = 0,
    read_end: usize = 0,
    oversized_records_since_warning: usize = 0,
    last_gap_warning_timestamp: i64 = 0,
    running: bool,

    const PollResult = struct {
        entry: ?LogEntry = null,
        bytes_processed: usize = 0,
    };

    pub fn init(allocator: Allocator) !JournalTailer {
        var record_buffer = try JournalRecordBuffer.init(allocator, max_journal_record_bytes);
        errdefer record_buffer.deinit(allocator);

        var process = ChildProcess.init(&[_][]const u8{
            "/usr/bin/journalctl", // absolute path: standard on Proxmox/modern systemd distros (/bin -> /usr/bin)
            "-f", // follow
            "-o", "json", // JSON output
            "--since", "now", // only new entries
        }, allocator);

        process.stdout_behavior = .Pipe;
        process.stderr_behavior = .Ignore;

        try process.spawn();
        errdefer {
            _ = process.kill() catch {};
            _ = process.wait() catch {};
        }

        // Set stdout to non-blocking so next() doesn't block the main loop
        const fd: std.posix.fd_t = process.stdout.?.handle;
        const flags = try std.posix.fcntl(fd, std.posix.F.GETFL, 0);
        _ = try std.posix.fcntl(fd, std.posix.F.SETFL, flags | @as(usize, @as(u32, @bitCast(std.posix.O{ .NONBLOCK = true }))));

        return JournalTailer{
            .allocator = allocator,
            .process = process,
            .record_buffer = record_buffer,
            .running = true,
        };
    }

    pub fn deinit(self: *JournalTailer) void {
        self.flushOversizedWarnings(true);
        self.running = false;
        _ = self.process.kill() catch {};
        _ = self.process.wait() catch {};
        self.record_buffer.deinit(self.allocator);
    }

    pub fn next(self: *JournalTailer, max_bytes: usize) !PollResult {
        self.flushOversizedWarnings(false);
        if (!self.running or max_bytes == 0) return .{};

        // Keep partial JSON across WouldBlock. Once a record exceeds the cap,
        // consume through its newline without retaining more bytes; reporting
        // the gap only at that boundary keeps the following record aligned.
        // The caller supplies the remaining collection-cycle byte budget so
        // discarded input and valid records share one bounded work allowance.
        var bytes_processed: usize = 0;
        var record_complete = false;
        read_record: while (bytes_processed < max_bytes) {
            if (self.read_start == self.read_end) {
                const read_limit = @min(self.read_buffer.len, max_bytes - bytes_processed);
                const bytes_read = self.process.stdout.?.read(self.read_buffer[0..read_limit]) catch |err| switch (err) {
                    error.WouldBlock => return .{ .bytes_processed = bytes_processed },
                    else => return err,
                };
                if (bytes_read == 0) {
                    const incomplete = self.record_buffer.hasPartialRecord();
                    self.record_buffer.discarding = false;
                    self.record_buffer.clearRecord();
                    self.running = false;
                    if (incomplete) return error.IncompleteJournalRecord;
                    return .{ .bytes_processed = bytes_processed };
                }
                self.read_start = 0;
                self.read_end = bytes_read;
            }

            const scan = scanJournalBytes(
                &self.record_buffer,
                self.read_buffer[self.read_start..self.read_end],
                max_bytes - bytes_processed,
            );
            self.read_start += scan.bytes_consumed;
            bytes_processed += scan.bytes_consumed;
            switch (scan.event orelse continue) {
                .record => {
                    record_complete = true;
                    break :read_record;
                },
                .oversized => self.noteOversizedRecord(),
            }
        }

        // No complete record within this cycle's work budget. The partial or
        // discard state, plus any unread chunk bytes, is retained for next time.
        if (!record_complete) return .{ .bytes_processed = bytes_processed };
        defer self.record_buffer.clearRecord();

        // Parse the complete bounded JSON record.
        const parsed = std.json.parseFromSlice(
            std.json.Value,
            self.allocator,
            self.record_buffer.bytes.items,
            .{},
        ) catch |err| {
            // Skip malformed JSON lines
            std.debug.print("Failed to parse journal JSON: {}\n", .{err});
            return .{ .bytes_processed = bytes_processed };
        };
        defer parsed.deinit();

        // A journald line is always a JSON object; anything else is malformed
        // input -- skip it rather than panic on the union tag.
        const obj = switch (parsed.value) {
            .object => |o| o,
            else => return .{ .bytes_processed = bytes_processed },
        };

        // Extract timestamp (microseconds -> seconds). A hostile/malformed
        // value must not abort the read loop, so fall back to now on any parse
        // failure, consistent with the PRIORITY/_PID fields below.
        const timestamp: i64 = blk: {
            if (jsonStr(obj.get("__REALTIME_TIMESTAMP"))) |ts| {
                if (std.fmt.parseInt(i64, ts, 10)) |usec| {
                    break :blk @divFloor(usec, 1_000_000);
                } else |_| {}
            }
            break :blk std.time.timestamp();
        };

        // Extract identity fields. Keep unit as the historical grouping key.
        const identifier: ?[]const u8 = if (jsonStr(obj.get("SYSLOG_IDENTIFIER"))) |id|
            try self.allocator.dupe(u8, id)
        else
            null;
        errdefer if (identifier) |value| self.allocator.free(value);

        const systemd_unit: ?[]const u8 = if (jsonStr(obj.get("_SYSTEMD_UNIT"))) |unit_name|
            try self.allocator.dupe(u8, unit_name)
        else
            null;
        errdefer if (systemd_unit) |value| self.allocator.free(value);

        const unit: ?[]const u8 = if (identifier) |value|
            try self.allocator.dupe(u8, value)
        else if (systemd_unit) |value|
            try self.allocator.dupe(u8, value)
        else
            null;
        errdefer if (unit) |value| self.allocator.free(value);

        // Extract priority (default to 6 = INFO)
        const priority: u8 = if (jsonStr(obj.get("PRIORITY"))) |prio|
            @intCast(std.fmt.parseInt(u8, prio, 10) catch 6)
        else
            6;

        // Extract message
        const message = if (jsonStr(obj.get("MESSAGE"))) |msg|
            try self.allocator.dupe(u8, msg)
        else
            try self.allocator.dupe(u8, "");
        errdefer self.allocator.free(message);

        // Extract PID
        const pid: ?u32 = if (jsonStr(obj.get("_PID"))) |pid_str|
            std.fmt.parseInt(u32, pid_str, 10) catch null
        else
            null;

        const source = try self.allocator.dupe(u8, "systemd");
        errdefer self.allocator.free(source);

        return .{
            .entry = LogEntry{
                .timestamp = timestamp,
                .source = source,
                .unit = unit,
                .identifier = identifier,
                .systemd_unit = systemd_unit,
                .priority = priority,
                .message = message,
                .pid = pid,
                .trace_id = try normalizeTrace(self.allocator, jsonStr(obj.get("TRACE_ID")) orelse jsonStr(obj.get("trace_id"))),
            },
            .bytes_processed = bytes_processed,
        };
    }

    fn noteOversizedRecord(self: *JournalTailer) void {
        self.oversized_records_since_warning += 1;
        self.flushOversizedWarnings(false);
    }

    fn flushOversizedWarnings(self: *JournalTailer, force: bool) void {
        if (self.oversized_records_since_warning == 0) return;
        const now = std.time.timestamp();
        if (!force and self.last_gap_warning_timestamp != 0 and
            now >= self.last_gap_warning_timestamp and
            now - self.last_gap_warning_timestamp < journal_gap_warning_interval_seconds)
        {
            return;
        }

        std.log.warn(
            "journal collection gap: discarded {d} record(s) larger than {d} bytes; repeated warnings are coalesced for {d} seconds",
            .{
                self.oversized_records_since_warning,
                max_journal_record_bytes,
                journal_gap_warning_interval_seconds,
            },
        );
        self.oversized_records_since_warning = 0;
        self.last_gap_warning_timestamp = now;
    }
};

/// Only explicit structured fields qualify; message text is never a trace ID.
pub fn normalizeTrace(a: Allocator, value: ?[]const u8) !?[]const u8 {
    const s = value orelse return null;
    if (s.len != 32) return null;
    var buf: [32]u8 = undefined;
    var nonzero = false;
    for (s, 0..) |ch, i| {
        if (!std.ascii.isHex(ch)) return null;
        buf[i] = std.ascii.toLower(ch);
        nonzero = nonzero or ch != '0';
    }
    return if (nonzero) try a.dupe(u8, &buf) else null;
}

/// File tailer with rotation detection
const FileTailer = struct {
    allocator: Allocator,
    path: []const u8,
    file: ?std.fs.File,
    inode: ?std.posix.ino_t,
    position: u64,
    line_buffer: std.ArrayList(u8),

    pub fn init(allocator: Allocator, path: []const u8) !FileTailer {
        const path_copy = try allocator.dupe(u8, path);
        errdefer allocator.free(path_copy);

        var tailer = FileTailer{
            .allocator = allocator,
            .path = path_copy,
            .file = null,
            .inode = null,
            .position = 0,
            .line_buffer = std.ArrayList(u8){},
        };

        // Try to open file (may not exist yet)
        tailer.openFile() catch |err| {
            std.debug.print("Failed to open log file {s}: {}\n", .{ path, err });
        };

        return tailer;
    }

    pub fn deinit(self: *FileTailer) void {
        if (self.file) |*f| {
            f.close();
        }
        self.allocator.free(self.path);
        self.line_buffer.deinit(self.allocator);
    }

    fn openFile(self: *FileTailer) !void {
        // Close existing file if open
        if (self.file) |*f| {
            f.close();
        }

        // Open file
        const file = try std.fs.cwd().openFile(self.path, .{ .mode = .read_only });
        const stat = try file.stat();

        // Seek to end for tailing
        try file.seekTo(stat.size);

        self.file = file;
        self.inode = stat.inode;
        self.position = stat.size;
    }

    fn checkRotation(self: *FileTailer) !void {
        // Check if file still exists and has same inode
        const stat = std.fs.cwd().statFile(self.path) catch |err| {
            if (err == error.FileNotFound) {
                // File removed, close current handle
                if (self.file) |*f| {
                    f.close();
                    self.file = null;
                    self.inode = null;
                }
            }
            return err;
        };

        // If inode changed, file was rotated
        if (self.inode) |old_inode| {
            if (stat.inode != old_inode) {
                std.debug.print("Detected log rotation on {s}\n", .{self.path});
                try self.openFile();
            }
        }
    }

    pub fn next(self: *FileTailer) !?LogEntry {
        // Ensure file is open
        if (self.file == null) {
            self.openFile() catch return null;
        }

        // Check for rotation
        self.checkRotation() catch {};

        if (self.file) |file| {
            self.line_buffer.clearRetainingCapacity();

            // Try to read a line
            file.deprecatedReader().streamUntilDelimiter(
                self.line_buffer.writer(self.allocator),
                '\n',
                null,
            ) catch |err| {
                if (err == error.EndOfStream) {
                    // No new data yet
                    return null;
                }
                return err;
            };

            // Update position
            self.position += self.line_buffer.items.len + 1; // +1 for newline

            // Create log entry
            const timestamp = std.time.timestamp();
            const source = try self.allocator.dupe(u8, self.path);
            const message = try self.allocator.dupe(u8, self.line_buffer.items);

            return LogEntry{
                .timestamp = timestamp,
                .source = source,
                .unit = null,
                .identifier = null,
                .systemd_unit = null,
                .priority = 6, // INFO level by default
                .message = message,
                .pid = null,
            };
        }

        return null;
    }
};

// Tests
fn pushRecordBytes(buffer: *JournalRecordBuffer, bytes: []const u8) ?JournalRecordBuffer.Event {
    for (bytes) |byte| if (buffer.push(byte)) |event| return event;
    return null;
}

test "journal record preserves fragments across WouldBlock boundaries" {
    const a = std.testing.allocator;
    var buffer = try JournalRecordBuffer.init(a, 64);
    defer buffer.deinit(a);

    try std.testing.expectEqual(@as(?JournalRecordBuffer.Event, null), pushRecordBytes(&buffer, "{\"MESSAGE\":"));
    try std.testing.expectEqualStrings("{\"MESSAGE\":", buffer.bytes.items);
    try std.testing.expectEqual(@as(?JournalRecordBuffer.Event, null), pushRecordBytes(&buffer, "\"split"));
    try std.testing.expectEqualStrings("{\"MESSAGE\":\"split", buffer.bytes.items);
    try std.testing.expectEqual(JournalRecordBuffer.Event.record, pushRecordBytes(&buffer, " record\"}\n").?);
    try std.testing.expectEqualStrings("{\"MESSAGE\":\"split record\"}", buffer.bytes.items);
}

test "journal record accepts the exact byte ceiling" {
    const a = std.testing.allocator;
    var buffer = try JournalRecordBuffer.init(a, 4);
    defer buffer.deinit(a);

    try std.testing.expectEqual(@as(?JournalRecordBuffer.Event, null), pushRecordBytes(&buffer, "abcd"));
    try std.testing.expectEqual(JournalRecordBuffer.Event.record, buffer.push('\n').?);
    try std.testing.expectEqualStrings("abcd", buffer.bytes.items);
    try std.testing.expectEqual(@as(usize, 4), buffer.bytes.capacity);
}

test "journal scanner reports a record ending at the exact work boundary" {
    const a = std.testing.allocator;
    var buffer = try JournalRecordBuffer.init(a, 8);
    defer buffer.deinit(a);

    const input = "abc\nnext\n";
    const first = scanJournalBytes(&buffer, input, 4);
    try std.testing.expectEqual(@as(usize, 4), first.bytes_consumed);
    try std.testing.expectEqual(JournalRecordBuffer.Event.record, first.event.?);
    try std.testing.expectEqualStrings("abc", buffer.bytes.items);

    buffer.clearRecord();
    const second = scanJournalBytes(&buffer, input[first.bytes_consumed..], input.len);
    try std.testing.expectEqual(JournalRecordBuffer.Event.record, second.event.?);
    try std.testing.expectEqualStrings("next", buffer.bytes.items);
}

test "oversized journal record discards through newline then recovers" {
    const a = std.testing.allocator;
    var buffer = try JournalRecordBuffer.init(a, 4);
    defer buffer.deinit(a);

    try std.testing.expectEqual(@as(?JournalRecordBuffer.Event, null), pushRecordBytes(&buffer, "abcde"));
    try std.testing.expect(buffer.discarding);
    try std.testing.expectEqual(@as(usize, 0), buffer.bytes.items.len);
    try std.testing.expectEqual(@as(?JournalRecordBuffer.Event, null), pushRecordBytes(&buffer, "still oversized"));
    try std.testing.expectEqual(JournalRecordBuffer.Event.oversized, buffer.push('\n').?);
    try std.testing.expect(!buffer.discarding);

    try std.testing.expectEqual(JournalRecordBuffer.Event.record, pushRecordBytes(&buffer, "ok\n").?);
    try std.testing.expectEqualStrings("ok", buffer.bytes.items);
}

test "journal record retained capacity never exceeds its ceiling" {
    const a = std.testing.allocator;
    var buffer = try JournalRecordBuffer.init(a, 7);
    defer buffer.deinit(a);

    for (0..10_000) |_| _ = buffer.push('x');
    try std.testing.expect(buffer.discarding);
    try std.testing.expectEqual(@as(usize, 0), buffer.bytes.items.len);
    try std.testing.expectEqual(@as(usize, 7), buffer.bytes.capacity);
    try std.testing.expectEqual(JournalRecordBuffer.Event.oversized, buffer.push('\n').?);
    buffer.clearRecord();
    try std.testing.expectEqual(@as(usize, 7), buffer.bytes.capacity);
}

test "LogEntry memory management" {
    const allocator = std.testing.allocator;

    var entry = LogEntry{
        .timestamp = 1234567890,
        .source = try allocator.dupe(u8, "systemd"),
        .unit = try allocator.dupe(u8, "nginx"),
        .identifier = try allocator.dupe(u8, "nginx"),
        .systemd_unit = try allocator.dupe(u8, "nginx.service"),
        .priority = 3,
        .message = try allocator.dupe(u8, "Test message"),
        .pid = 1234,
    };
    defer entry.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 1234567890), entry.timestamp);
    try std.testing.expectEqualStrings("systemd", entry.source);
    try std.testing.expectEqualStrings("nginx", entry.unit.?);
    try std.testing.expectEqualStrings("nginx", entry.identifier.?);
    try std.testing.expectEqualStrings("nginx.service", entry.systemd_unit.?);
}

test "LogSource types" {
    const allocator = std.testing.allocator;

    const systemd_source = LogSource.systemd;
    try std.testing.expect(systemd_source == .systemd);

    const file_path = try allocator.dupe(u8, "/var/log/test.log");
    defer allocator.free(file_path);
    const file_source = LogSource{ .file = file_path };
    try std.testing.expectEqualStrings("/var/log/test.log", file_source.file);
}

test "FileTailer reads appended lines" {
    const allocator = std.testing.allocator;

    // Create a temp log file with initial content
    const test_path = "/tmp/sermon_test_filetailer.log";
    {
        const file = try std.fs.cwd().createFile(test_path, .{});
        defer file.close();
        try file.writeAll("Initial line\n");
    }
    defer std.fs.cwd().deleteFile(test_path) catch {};

    // Init tailer (seeks to end, so initial content is skipped)
    var tailer = try FileTailer.init(allocator, test_path);
    defer tailer.deinit();

    // Nothing new yet
    try std.testing.expect(try tailer.next() == null);

    // Append a line
    {
        const file = try std.fs.cwd().openFile(test_path, .{ .mode = .write_only });
        defer file.close();
        try file.seekFromEnd(0);
        try file.writeAll("Appended line\n");
    }

    // Should read the appended line
    var entry = (try tailer.next()) orelse return error.TestExpectedEntry;
    defer entry.deinit(allocator);

    try std.testing.expectEqualStrings("Appended line", entry.message);
    try std.testing.expectEqualStrings(test_path, entry.source);
    try std.testing.expectEqual(@as(u8, 6), entry.priority);
    try std.testing.expect(entry.unit == null);
}

test "structured trace IDs normalize, absent invalid and zero IDs stay null" {
    const a = std.testing.allocator;
    const trace = (try normalizeTrace(a, "ABCDEF0123456789ABCDEF0123456789")).?;
    defer a.free(trace);
    try std.testing.expectEqualStrings("abcdef0123456789abcdef0123456789", trace);
    try std.testing.expect((try normalizeTrace(a, null)) == null);
    try std.testing.expect((try normalizeTrace(a, "00000000000000000000000000000000")) == null);
    try std.testing.expect((try normalizeTrace(a, "trace_id=abcdef0123456789abcdef0123456789")) == null);
}
