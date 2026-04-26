const std = @import("std");
const commands = @import("commands.zig");
const output = @import("output.zig");
const storage_mod = @import("storage");

const Allocator = std.mem.Allocator;

const default_config_path = "~/.config/sermon/config.toml";
const default_db_path = "~/.local/share/sermon/metrics.db";

const usage =
    \\Usage: sermon [OPTIONS] <COMMAND> [COMMAND_OPTIONS]
    \\
    \\Options:
    \\  --config <path>       Path to config file (default: ~/.config/sermon/config.toml)
    \\  --format <format>     Output format: table, json, csv (default: table)
    \\  --db <path>           Database path (overrides config)
    \\  -h, --help            Show this help message
    \\
    \\Commands:
    \\  status                Show current system status
    \\  metrics               Show metrics over time
    \\    --period <duration>   Time period (e.g., 1h, 30m, 7d) (default: 1h)
    \\  processes             List processes
    \\    --sort <field>        Sort by: cpu, mem, name (default: cpu)
    \\    --filter <pattern>    Filter by name/cmdline
    \\  logs                  Query logs
    \\    --unit <name>         Filter by systemd unit
    \\    --since <duration>    Time period (e.g., 1h, 30m)
    \\    --priority <level>    Filter by priority (emerg, alert, crit, err, warn, notice, info, debug)
    \\  query <sql>           Run raw SQL query
    \\
    \\Examples:
    \\  sermon status
    \\  sermon metrics --period 1h --format json
    \\  sermon processes --sort mem --filter nginx
    \\  sermon logs --unit nginx --since 1h --priority err
    \\  sermon query "SELECT * FROM metrics ORDER BY timestamp DESC LIMIT 10"
    \\
;

fn getStdout() std.fs.File.DeprecatedWriter {
    const f: std.fs.File = .{ .handle = std.posix.STDOUT_FILENO };
    return f.deprecatedWriter();
}

fn getStderr() std.fs.File.DeprecatedWriter {
    const f: std.fs.File = .{ .handle = std.posix.STDERR_FILENO };
    return f.deprecatedWriter();
}

fn printUsage() void {
    const stderr = getStderr();
    stderr.writeAll(usage) catch {};
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    // Skip program name
    _ = args.skip();

    // Parse global options
    var config_path: ?[]const u8 = null;
    var db_path: ?[]const u8 = null;
    var format = output.OutputFormat.table;

    var cmd_name: ?[]const u8 = null;
    var cmd_args = std.ArrayList([]const u8){};
    defer cmd_args.deinit(allocator);

    // Parse arguments
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage();
            return;
        } else if (std.mem.eql(u8, arg, "--config")) {
            config_path = args.next() orelse {
                std.debug.print("Error: --config requires a path\n", .{});
                printUsage();
                return error.InvalidArguments;
            };
        } else if (std.mem.eql(u8, arg, "--db")) {
            db_path = args.next() orelse {
                std.debug.print("Error: --db requires a path\n", .{});
                printUsage();
                return error.InvalidArguments;
            };
        } else if (std.mem.eql(u8, arg, "--format")) {
            const format_str = args.next() orelse {
                std.debug.print("Error: --format requires a value\n", .{});
                printUsage();
                return error.InvalidArguments;
            };
            format = parseFormat(format_str) catch {
                std.debug.print("Error: invalid format '{s}' (use: table, json, csv)\n", .{format_str});
                return error.InvalidArguments;
            };
        } else if (cmd_name == null) {
            cmd_name = arg;
        } else {
            try cmd_args.append(allocator, arg);
        }
    }

    // Check if command was provided
    const command = cmd_name orelse {
        std.debug.print("Error: no command specified\n\n", .{});
        printUsage();
        return error.NoCommand;
    };

    // Resolve database path
    const final_db_path = db_path orelse try expandPath(allocator, default_db_path);
    defer if (db_path == null) allocator.free(final_db_path);

    // Initialize storage (read-only, retry if agent briefly holds the lock)
    var storage = storage_mod.Storage.initReadOnly(allocator, final_db_path) catch blk: {
        for (0..3) |_| {
            std.Thread.sleep(100 * std.time.ns_per_ms);
            break :blk storage_mod.Storage.initReadOnly(allocator, final_db_path) catch continue;
        }
        std.debug.print("Error: could not open database (agent may be writing). Try again.\n", .{});
        return;
    };
    defer storage.deinit();

    // Route to command handler
    if (std.mem.eql(u8, command, "status")) {
        try handleStatus(allocator, &storage, format);
    } else if (std.mem.eql(u8, command, "metrics")) {
        try handleMetrics(allocator, &storage, format, cmd_args.items);
    } else if (std.mem.eql(u8, command, "processes")) {
        try handleProcesses(allocator, &storage, format, cmd_args.items);
    } else if (std.mem.eql(u8, command, "logs")) {
        try handleLogs(allocator, &storage, format, cmd_args.items);
    } else if (std.mem.eql(u8, command, "query")) {
        try handleQuery(allocator, &storage, format, cmd_args.items);
    } else {
        std.debug.print("Error: unknown command '{s}'\n\n", .{command});
        printUsage();
        return error.UnknownCommand;
    }
}

fn parseFormat(format_str: []const u8) !output.OutputFormat {
    if (std.mem.eql(u8, format_str, "table")) return .table;
    if (std.mem.eql(u8, format_str, "json")) return .json;
    if (std.mem.eql(u8, format_str, "csv")) return .csv;
    return error.InvalidFormat;
}

fn expandPath(allocator: Allocator, path: []const u8) ![]const u8 {
    if (path.len > 0 and path[0] == '~') {
        const home = std.posix.getenv("HOME") orelse return error.NoHomeDir;
        return std.fmt.allocPrint(allocator, "{s}{s}", .{ home, path[1..] });
    }
    return allocator.dupe(u8, path);
}

fn handleStatus(
    allocator: Allocator,
    storage: *commands.Storage,
    format: output.OutputFormat,
) !void {
    try commands.cmdStatus(allocator, storage, format);
}

fn handleMetrics(
    allocator: Allocator,
    storage: *commands.Storage,
    format: output.OutputFormat,
    args: []const []const u8,
) !void {
    var period_seconds: i64 = 3600; // Default: 1 hour

    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        const arg = args[i];

        if (std.mem.eql(u8, arg, "--period")) {
            i += 1;
            if (i >= args.len) {
                std.debug.print("Error: --period requires a duration\n", .{});
                return error.InvalidArguments;
            }
            period_seconds = try commands.parseDuration(args[i]);
        } else {
            std.debug.print("Error: unknown option '{s}' for metrics command\n", .{arg});
            return error.InvalidArguments;
        }
    }

    try commands.cmdMetrics(allocator, storage, format, period_seconds);
}

fn handleProcesses(
    allocator: Allocator,
    storage: *commands.Storage,
    format: output.OutputFormat,
    args: []const []const u8,
) !void {
    var sort_by = commands.ProcessSort.cpu;
    var filter_pattern: ?[]const u8 = null;

    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        const arg = args[i];

        if (std.mem.eql(u8, arg, "--sort")) {
            i += 1;
            if (i >= args.len) {
                std.debug.print("Error: --sort requires a field\n", .{});
                return error.InvalidArguments;
            }
            const sort_str = args[i];
            if (std.mem.eql(u8, sort_str, "cpu")) {
                sort_by = .cpu;
            } else if (std.mem.eql(u8, sort_str, "mem")) {
                sort_by = .mem;
            } else if (std.mem.eql(u8, sort_str, "name")) {
                sort_by = .name;
            } else {
                std.debug.print("Error: invalid sort field '{s}' (use: cpu, mem, name)\n", .{sort_str});
                return error.InvalidArguments;
            }
        } else if (std.mem.eql(u8, arg, "--filter")) {
            i += 1;
            if (i >= args.len) {
                std.debug.print("Error: --filter requires a pattern\n", .{});
                return error.InvalidArguments;
            }
            filter_pattern = args[i];
        } else {
            std.debug.print("Error: unknown option '{s}' for processes command\n", .{arg});
            return error.InvalidArguments;
        }
    }

    try commands.cmdProcesses(allocator, storage, format, sort_by, filter_pattern);
}

fn handleLogs(
    allocator: Allocator,
    storage: *commands.Storage,
    format: output.OutputFormat,
    args: []const []const u8,
) !void {
    var unit: ?[]const u8 = null;
    var since_seconds: ?i64 = null;
    var priority_str: ?[]const u8 = null;

    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        const arg = args[i];

        if (std.mem.eql(u8, arg, "--unit")) {
            i += 1;
            if (i >= args.len) {
                std.debug.print("Error: --unit requires a name\n", .{});
                return error.InvalidArguments;
            }
            unit = args[i];
        } else if (std.mem.eql(u8, arg, "--since")) {
            i += 1;
            if (i >= args.len) {
                std.debug.print("Error: --since requires a duration\n", .{});
                return error.InvalidArguments;
            }
            since_seconds = try commands.parseDuration(args[i]);
        } else if (std.mem.eql(u8, arg, "--priority")) {
            i += 1;
            if (i >= args.len) {
                std.debug.print("Error: --priority requires a level\n", .{});
                return error.InvalidArguments;
            }
            priority_str = args[i];
        } else {
            std.debug.print("Error: unknown option '{s}' for logs command\n", .{arg});
            return error.InvalidArguments;
        }
    }

    try commands.cmdLogs(allocator, storage, format, unit, since_seconds, priority_str);
}

fn handleQuery(
    allocator: Allocator,
    storage: *commands.Storage,
    format: output.OutputFormat,
    args: []const []const u8,
) !void {
    if (args.len == 0) {
        std.debug.print("Error: query command requires a SQL string\n", .{});
        return error.InvalidArguments;
    }

    // Join all args as SQL (allows for multi-word queries without quotes in shell)
    const sql = try std.mem.join(allocator, " ", args);
    defer allocator.free(sql);

    try commands.cmdQuery(allocator, storage, format, sql);
}

test "parseFormat" {
    try std.testing.expectEqual(output.OutputFormat.table, try parseFormat("table"));
    try std.testing.expectEqual(output.OutputFormat.json, try parseFormat("json"));
    try std.testing.expectEqual(output.OutputFormat.csv, try parseFormat("csv"));
    try std.testing.expectError(error.InvalidFormat, parseFormat("xml"));
}
