const std = @import("std");
const Allocator = std.mem.Allocator;
const ChildProcess = std.process.Child;

/// Log entry structure - consumed by storage layer
pub const LogEntry = struct {
    timestamp: i64, // Unix timestamp (seconds)
    source: []const u8, // "systemd" or file path
    unit: ?[]const u8, // systemd unit name (null for file logs)
    priority: u8, // syslog priority 0-7 (0=emerg, 7=debug)
    message: []const u8, // log message content
    pid: ?u32, // process ID if available

    pub fn deinit(self: *LogEntry, allocator: Allocator) void {
        allocator.free(self.source);
        if (self.unit) |unit| {
            allocator.free(unit);
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

    /// Get next log entry from any source (blocks until available)
    /// Returns null on permanent failure
    pub fn next(self: *LogTailer) !?LogEntry {
        // Simple round-robin: check journal first, then files
        // In a production system, this would use select/poll for efficiency

        if (self.journal_tailer) |jt| {
            if (try jt.next()) |entry| {
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
    line_buffer: std.ArrayList(u8),
    running: bool,

    pub fn init(allocator: Allocator) !JournalTailer {
        var process = ChildProcess.init(&[_][]const u8{
            "journalctl",
            "-f", // follow
            "-o", "json", // JSON output
            "--since", "now", // only new entries
        }, allocator);

        process.stdout_behavior = .Pipe;
        process.stderr_behavior = .Ignore;

        try process.spawn();

        // Set stdout to non-blocking so next() doesn't block the main loop
        const fd: std.posix.fd_t = process.stdout.?.handle;
        const flags = try std.posix.fcntl(fd, std.posix.F.GETFL, 0);
        _ = try std.posix.fcntl(fd, std.posix.F.SETFL, flags | @as(usize, @as(u32, @bitCast(std.posix.O{ .NONBLOCK = true }))));

        return JournalTailer{
            .allocator = allocator,
            .process = process,
            .line_buffer = std.ArrayList(u8){},
            .running = true,
        };
    }

    pub fn deinit(self: *JournalTailer) void {
        self.running = false;
        _ = self.process.kill() catch {};
        _ = self.process.wait() catch {};
        self.line_buffer.deinit(self.allocator);
    }

    pub fn next(self: *JournalTailer) !?LogEntry {
        if (!self.running) return null;

        self.line_buffer.clearRetainingCapacity();

        // Read one line of JSON
        const reader = self.process.stdout.?.deprecatedReader();
        reader.streamUntilDelimiter(
            self.line_buffer.writer(self.allocator),
            '\n',
            null,
        ) catch |err| {
            if (err == error.EndOfStream) {
                self.running = false;
                return null;
            }
            if (err == error.WouldBlock) return null;
            return err;
        };

        // Parse JSON
        const parsed = std.json.parseFromSlice(
            std.json.Value,
            self.allocator,
            self.line_buffer.items,
            .{},
        ) catch |err| {
            // Skip malformed JSON lines
            std.debug.print("Failed to parse journal JSON: {}\n", .{err});
            return null;
        };
        defer parsed.deinit();

        const obj = parsed.value.object;

        // Extract timestamp (microseconds -> seconds)
        const timestamp: i64 = if (obj.get("__REALTIME_TIMESTAMP")) |ts|
            @divFloor(try std.fmt.parseInt(i64, ts.string, 10), 1_000_000)
        else
            std.time.timestamp();

        // Extract unit name
        const unit: ?[]const u8 = if (obj.get("SYSLOG_IDENTIFIER")) |id|
            try self.allocator.dupe(u8, id.string)
        else if (obj.get("_SYSTEMD_UNIT")) |unit_name|
            try self.allocator.dupe(u8, unit_name.string)
        else
            null;

        // Extract priority (default to 6 = INFO)
        const priority: u8 = if (obj.get("PRIORITY")) |prio|
            @intCast(std.fmt.parseInt(u8, prio.string, 10) catch 6)
        else
            6;

        // Extract message
        const message = if (obj.get("MESSAGE")) |msg|
            try self.allocator.dupe(u8, msg.string)
        else
            try self.allocator.dupe(u8, "");

        // Extract PID
        const pid: ?u32 = if (obj.get("_PID")) |pid_str|
            std.fmt.parseInt(u32, pid_str.string, 10) catch null
        else
            null;

        const source = try self.allocator.dupe(u8, "systemd");

        return LogEntry{
            .timestamp = timestamp,
            .source = source,
            .unit = unit,
            .priority = priority,
            .message = message,
            .pid = pid,
        };
    }
};

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
                .priority = 6, // INFO level by default
                .message = message,
                .pid = null,
            };
        }

        return null;
    }
};

// Tests
test "LogEntry memory management" {
    const allocator = std.testing.allocator;

    var entry = LogEntry{
        .timestamp = 1234567890,
        .source = try allocator.dupe(u8, "systemd"),
        .unit = try allocator.dupe(u8, "nginx.service"),
        .priority = 3,
        .message = try allocator.dupe(u8, "Test message"),
        .pid = 1234,
    };
    defer entry.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 1234567890), entry.timestamp);
    try std.testing.expectEqualStrings("systemd", entry.source);
    try std.testing.expectEqualStrings("nginx.service", entry.unit.?);
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
