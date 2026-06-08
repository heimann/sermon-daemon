//! On-demand query path for the parquet hot tier (plan 25).
//!
//! Queries are rare and human-triggered (sermon query, the stdio MCP). Each one
//! brings DuckDB up TRANSIENTLY in-memory, builds a per-table compatibility
//! VIEW named exactly like today's tables (metrics/processes/disks/containers/
//! logs), runs the SQL, and exits. DuckDB is never resident.
//!
//! Each view is:
//!
//!     read_parquet('<root>/<table>/**/*.parquet', hive_partitioning=true)
//!       UNION ALL <table>_staging
//!
//! where `<table>_staging` is a TEMP table loaded from a Zig-decoded staging
//! snapshot (the rows not yet rolled to parquet).
//!
//! CRITICAL ORDERING (no double-count, no miss):
//!   We enumerate/snapshot the parquet file-set FIRST, then snapshot staging
//!   SECOND. A roll that runs concurrently moves rows from staging to a new
//!   parquet file and then truncates staging. By reading parquet before
//!   staging, the worst case is:
//!     - a brand-new parquet file appears AFTER we listed the glob -> we miss
//!       it, but its rows are still present in our (older) staging snapshot, so
//!       they are counted exactly once via staging.
//!   We can NEVER double-count, because a row can only be in our parquet list
//!   OR our staging snapshot, never both: it is in parquet only if the file
//!   existed before our staging read, in which case the roll had already
//!   truncated it out of staging.
//!
//! The query path is read-only and opens no control channel to the daemon - the
//! daemon stays write-only.

const std = @import("std");
const Allocator = std.mem.Allocator;
const fs = std.fs;

const c = @cImport({
    @cInclude("duckdb.h");
});

const staging = @import("staging");
const roll = @import("roll");
const collector = @import("collector");
const logs = @import("logs");
const proxmox = @import("proxmox");

const Table = staging.Table;

pub const QueryError = error{
    DatabaseError,
    ConnectionError,
    SchemaError,
    QueryError,
    OutOfMemory,
};

/// Mirror of storage.QueryResult so the CLI's rawQuery callers are unchanged.
pub const QueryResult = struct {
    columns: [][]const u8,
    rows: [][]?[]const u8,
    allocator: Allocator,

    pub fn deinit(self: *QueryResult) void {
        for (self.columns) |col| self.allocator.free(col);
        self.allocator.free(self.columns);
        for (self.rows) |row| {
            for (row) |cell| {
                if (cell) |v| self.allocator.free(v);
            }
            self.allocator.free(row);
        }
        self.allocator.free(self.rows);
    }
};

/// An on-demand query handle over the parquet hot tier. Open it, run queries,
/// then deinit - it owns a transient in-memory DuckDB plus the loaded staging
/// snapshots. Same accessor surface as the resident Storage query path so the
/// CLI is unchanged.
pub const ParquetQuery = struct {
    allocator: Allocator,
    db: c.duckdb_database,
    conn: c.duckdb_connection,

    pub fn deinit(self: *ParquetQuery) void {
        c.duckdb_disconnect(&self.conn);
        c.duckdb_close(&self.db);
    }

    pub fn rawQuery(self: *ParquetQuery, sql: []const u8) !QueryResult {
        const c_sql = try self.allocator.dupeZ(u8, sql);
        defer self.allocator.free(c_sql);

        var result: c.duckdb_result = undefined;
        const state = c.duckdb_query(self.conn, c_sql.ptr, &result);
        defer c.duckdb_destroy_result(&result);
        if (state == c.DuckDBError) {
            std.log.err("parquet rawQuery error: {s}", .{c.duckdb_result_error(&result)});
            return QueryError.QueryError;
        }

        const col_count = c.duckdb_column_count(&result);
        const row_count = c.duckdb_row_count(&result);

        const columns = try self.allocator.alloc([]const u8, col_count);
        errdefer self.allocator.free(columns);
        var col_idx: usize = 0;
        while (col_idx < col_count) : (col_idx += 1) {
            const name = c.duckdb_column_name(&result, col_idx);
            columns[col_idx] = try self.allocator.dupe(u8, std.mem.span(name));
        }

        const rows = try self.allocator.alloc([]?[]const u8, row_count);
        errdefer self.allocator.free(rows);
        var row_idx: usize = 0;
        while (row_idx < row_count) : (row_idx += 1) {
            rows[row_idx] = try self.allocator.alloc(?[]const u8, col_count);
            col_idx = 0;
            while (col_idx < col_count) : (col_idx += 1) {
                if (c.duckdb_value_is_null(&result, col_idx, row_idx)) {
                    rows[row_idx][col_idx] = null;
                } else {
                    const v = c.duckdb_value_varchar(&result, col_idx, row_idx);
                    rows[row_idx][col_idx] = try self.allocator.dupe(u8, std.mem.span(v));
                }
            }
        }

        return QueryResult{ .columns = columns, .rows = rows, .allocator = self.allocator };
    }

    // -- Typed accessors mirroring storage.zig (over the views) --------------

    pub fn getLatestMetrics(self: *ParquetQuery) !?collector.SystemMetrics {
        var result: c.duckdb_result = undefined;
        const sql = "SELECT * FROM metrics ORDER BY timestamp DESC LIMIT 1";
        if (c.duckdb_query(self.conn, sql, &result) == c.DuckDBError) {
            std.log.err("getLatestMetrics error: {s}", .{c.duckdb_result_error(&result)});
            c.duckdb_destroy_result(&result);
            return QueryError.QueryError;
        }
        defer c.duckdb_destroy_result(&result);
        if (c.duckdb_row_count(&result) == 0) return null;
        return readMetricsRow(&result, 0);
    }

    pub fn getMetricsRange(self: *ParquetQuery, since: i64, until: i64) ![]collector.SystemMetrics {
        // Compare on epoch seconds rather than to_timestamp($1): the parquet
        // and staging timestamps are plain TIMESTAMP (no tz), while
        // to_timestamp returns TIMESTAMPTZ and the implicit tz conversion would
        // shift the bounds. epoch() yields the tz-free unix seconds we bound.
        const sql = "SELECT * FROM metrics WHERE epoch(timestamp) >= $1 AND epoch(timestamp) <= $2 ORDER BY timestamp";
        var stmt: c.duckdb_prepared_statement = undefined;
        if (c.duckdb_prepare(self.conn, sql, &stmt) == c.DuckDBError) return QueryError.QueryError;
        defer c.duckdb_destroy_prepare(&stmt);
        _ = c.duckdb_bind_int64(stmt, 1, since);
        _ = c.duckdb_bind_int64(stmt, 2, until);

        var result: c.duckdb_result = undefined;
        if (c.duckdb_execute_prepared(stmt, &result) == c.DuckDBError) {
            c.duckdb_destroy_result(&result);
            return QueryError.QueryError;
        }
        defer c.duckdb_destroy_result(&result);

        const n = c.duckdb_row_count(&result);
        if (n == 0) return &[_]collector.SystemMetrics{};
        const out = try self.allocator.alloc(collector.SystemMetrics, n);
        errdefer self.allocator.free(out);
        var i: usize = 0;
        while (i < n) : (i += 1) out[i] = readMetricsRow(&result, i);
        return out;
    }

    pub fn getProcesses(self: *ParquetQuery, timestamp: ?i64) ![]collector.ProcessInfo {
        const sql = if (timestamp) |_|
            "SELECT * FROM processes WHERE timestamp = $1 ORDER BY cpu_percent DESC"
        else
            "SELECT * FROM processes WHERE timestamp = (SELECT MAX(timestamp) FROM processes) ORDER BY cpu_percent DESC";

        var result: c.duckdb_result = undefined;
        const state = if (timestamp) |ts| blk: {
            var stmt: c.duckdb_prepared_statement = undefined;
            if (c.duckdb_prepare(self.conn, sql.ptr, &stmt) == c.DuckDBError) return QueryError.QueryError;
            defer c.duckdb_destroy_prepare(&stmt);
            _ = c.duckdb_bind_timestamp(stmt, 1, .{ .micros = ts * 1_000_000 });
            break :blk c.duckdb_execute_prepared(stmt, &result);
        } else c.duckdb_query(self.conn, sql.ptr, &result);
        defer c.duckdb_destroy_result(&result);

        if (state == c.DuckDBError) {
            std.log.err("getProcesses error: {s}", .{c.duckdb_result_error(&result)});
            return QueryError.QueryError;
        }

        const n = c.duckdb_row_count(&result);
        if (n == 0) return &[_]collector.ProcessInfo{};
        const out = try self.allocator.alloc(collector.ProcessInfo, n);
        errdefer self.allocator.free(out);
        var filled: usize = 0;
        errdefer for (out[0..filled]) |*p| freeProcess(self.allocator, p);
        var i: usize = 0;
        while (i < n) : (i += 1) {
            out[i] = try readProcessRow(self.allocator, &result, i);
            filled += 1;
        }
        return out;
    }

    pub fn getDisks(self: *ParquetQuery, timestamp: ?i64) ![]collector.DiskInfo {
        const sql = if (timestamp) |_|
            "SELECT * FROM disks WHERE timestamp = $1 ORDER BY mount_point"
        else
            "SELECT * FROM disks WHERE timestamp = (SELECT MAX(timestamp) FROM disks) ORDER BY mount_point";

        var result: c.duckdb_result = undefined;
        const state = if (timestamp) |ts| blk: {
            var stmt: c.duckdb_prepared_statement = undefined;
            if (c.duckdb_prepare(self.conn, sql.ptr, &stmt) == c.DuckDBError) return QueryError.QueryError;
            defer c.duckdb_destroy_prepare(&stmt);
            _ = c.duckdb_bind_timestamp(stmt, 1, .{ .micros = ts * 1_000_000 });
            break :blk c.duckdb_execute_prepared(stmt, &result);
        } else c.duckdb_query(self.conn, sql.ptr, &result);
        defer c.duckdb_destroy_result(&result);

        if (state == c.DuckDBError) return QueryError.QueryError;

        const n = c.duckdb_row_count(&result);
        if (n == 0) return &[_]collector.DiskInfo{};
        const out = try self.allocator.alloc(collector.DiskInfo, n);
        errdefer self.allocator.free(out);
        var filled: usize = 0;
        errdefer for (out[0..filled]) |*d| freeDisk(self.allocator, d);
        var i: usize = 0;
        while (i < n) : (i += 1) {
            out[i] = try readDiskRow(self.allocator, &result, i);
            filled += 1;
        }
        return out;
    }

    pub fn queryLogs(self: *ParquetQuery, since: ?i64, unit: ?[]const u8, priority: ?u8) ![]logs.LogEntry {
        // epoch(timestamp) comparison for the same tz reason as getMetricsRange.
        const sql = "SELECT timestamp, source, unit, identifier, systemd_unit, priority, message, pid FROM logs WHERE ($1::BIGINT IS NULL OR epoch(timestamp) >= $1) AND ($2::VARCHAR IS NULL OR unit = $2) AND ($3::INTEGER IS NULL OR priority = $3) ORDER BY timestamp DESC";
        var stmt: c.duckdb_prepared_statement = undefined;
        if (c.duckdb_prepare(self.conn, sql, &stmt) == c.DuckDBError) return QueryError.QueryError;
        defer c.duckdb_destroy_prepare(&stmt);

        if (since) |s| {
            _ = c.duckdb_bind_int64(stmt, 1, s);
        } else _ = c.duckdb_bind_null(stmt, 1);
        if (unit) |u| {
            _ = c.duckdb_bind_varchar_length(stmt, 2, u.ptr, u.len);
        } else _ = c.duckdb_bind_null(stmt, 2);
        if (priority) |p| {
            _ = c.duckdb_bind_uint8(stmt, 3, p);
        } else _ = c.duckdb_bind_null(stmt, 3);

        var result: c.duckdb_result = undefined;
        if (c.duckdb_execute_prepared(stmt, &result) == c.DuckDBError) {
            std.log.err("queryLogs error: {s}", .{c.duckdb_result_error(&result)});
            c.duckdb_destroy_result(&result);
            return QueryError.QueryError;
        }
        defer c.duckdb_destroy_result(&result);

        const n = c.duckdb_row_count(&result);
        if (n == 0) return &[_]logs.LogEntry{};
        const out = try self.allocator.alloc(logs.LogEntry, n);
        errdefer self.allocator.free(out);
        var filled: usize = 0;
        errdefer for (out[0..filled]) |*e| e.deinit(self.allocator);
        var i: usize = 0;
        while (i < n) : (i += 1) {
            out[i] = try readLogRow(self.allocator, &result, i);
            filled += 1;
        }
        return out;
    }
};

// ============================================================================
// Open + view construction
// ============================================================================

/// Open a transient in-memory DuckDB and build the compatibility views over the
/// parquet glob UNION ALL the decoded staging snapshot, for every table.
pub fn initParquetQuery(allocator: Allocator, root_dir: []const u8) !ParquetQuery {
    var db: c.duckdb_database = undefined;
    if (c.duckdb_open(":memory:", &db) == c.DuckDBError) return QueryError.DatabaseError;
    errdefer c.duckdb_close(&db);

    var conn: c.duckdb_connection = undefined;
    if (c.duckdb_connect(db, &conn) == c.DuckDBError) return QueryError.ConnectionError;
    errdefer c.duckdb_disconnect(&conn);

    var pq = ParquetQuery{ .allocator = allocator, .db = db, .conn = conn };

    for (Table.all) |table| {
        // ORDER MATTERS: read the parquet file-set first (existence check on
        // the glob), THEN snapshot staging. See the module-level comment.
        const has_parquet = try tableHasParquet(allocator, root_dir, table);

        try createStagingTemp(allocator, &pq, root_dir, table);
        try createView(allocator, &pq, root_dir, table, has_parquet);
    }

    return pq;
}

/// True if at least one parquet file exists for this table. We also ensure the
/// table's directory exists so the read_parquet glob always resolves (an empty
/// glob otherwise errors in DuckDB).
fn tableHasParquet(allocator: Allocator, root_dir: []const u8, table: Table) !bool {
    const dir_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ root_dir, table.name() });
    defer allocator.free(dir_path);

    var dir = fs.openDirAbsolute(dir_path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer dir.close();

    var walker = try dir.walk(allocator);
    defer walker.deinit();
    while (try walker.next()) |entry| {
        if (entry.kind == .file and std.mem.endsWith(u8, entry.basename, ".parquet")) {
            return true;
        }
    }
    return false;
}

/// CREATE TEMP TABLE <table>_staging and load the (pre-roll) staging snapshot
/// into it via the appender. Empty staging yields an empty temp table.
fn createStagingTemp(allocator: Allocator, pq: *ParquetQuery, root_dir: []const u8, table: Table) !void {
    const create_sql = try stagingTempDdl(allocator, table);
    defer allocator.free(create_sql);

    var result: c.duckdb_result = undefined;
    if (c.duckdb_query(pq.conn, create_sql.ptr, &result) == c.DuckDBError) {
        std.log.err("create staging temp error: {s}", .{c.duckdb_result_error(&result)});
        c.duckdb_destroy_result(&result);
        return QueryError.SchemaError;
    }
    c.duckdb_destroy_result(&result);

    var snap = try staging.StagingReader.read(allocator, root_dir, table);
    defer snap.deinit();
    if (snap.rowCount() == 0) return;

    const temp_name = try std.fmt.allocPrintSentinel(allocator, "{s}_staging", .{table.name()}, 0);
    defer allocator.free(temp_name);

    var appender: c.duckdb_appender = undefined;
    if (c.duckdb_appender_create(pq.conn, null, temp_name.ptr, &appender) == c.DuckDBError) {
        return QueryError.SchemaError;
    }
    defer _ = c.duckdb_appender_destroy(&appender);

    _ = try roll.appendSnapshotTo(@ptrCast(appender), &snap);

    if (c.duckdb_appender_flush(appender) == c.DuckDBError) {
        std.log.err("staging temp flush error: {s}", .{c.duckdb_appender_error(appender)});
        return QueryError.SchemaError;
    }
}

/// Explicit, ordered column list for a table (matches storage.zig / the roll's
/// CREATE TABLE and the row readers). Used on both branches of the UNION so the
/// hive partition columns don't widen the parquet side.
fn columnList(table: Table) []const u8 {
    return switch (table) {
        .metrics => "timestamp, cpu_percent, cpu_user, cpu_system, cpu_iowait, mem_total, mem_used, mem_percent, swap_total, swap_used",
        .processes => "timestamp, pid, name, cmdline, state, cpu_percent, mem_rss, threads, username, io_read_bytes, io_write_bytes, cgroup, unit",
        .disks => "timestamp, mount_point, filesystem, total_bytes, used_bytes, percent",
        .containers => "timestamp, vmid, name, node, type, status, maxmem, maxcpu, uptime",
        .logs => "timestamp, source, unit, identifier, systemd_unit, priority, message, pid",
    };
}

/// DDL for the staging temp table - same columns/types as the parquet schema so
/// the UNION ALL type-checks.
fn stagingTempDdl(allocator: Allocator, table: Table) ![:0]u8 {
    const cols = switch (table) {
        .metrics => "timestamp TIMESTAMP, cpu_percent REAL, cpu_user REAL, cpu_system REAL, cpu_iowait REAL, mem_total BIGINT, mem_used BIGINT, mem_percent REAL, swap_total BIGINT, swap_used BIGINT",
        .processes => "timestamp TIMESTAMP, pid INTEGER, name VARCHAR, cmdline VARCHAR, state CHAR(1), cpu_percent REAL, mem_rss BIGINT, threads INTEGER, username VARCHAR, io_read_bytes BIGINT, io_write_bytes BIGINT, cgroup VARCHAR, unit VARCHAR",
        .disks => "timestamp TIMESTAMP, mount_point VARCHAR, filesystem VARCHAR, total_bytes BIGINT, used_bytes BIGINT, percent REAL",
        .containers => "timestamp TIMESTAMP, vmid INTEGER, name VARCHAR, node VARCHAR, type VARCHAR, status VARCHAR, maxmem BIGINT, maxcpu DOUBLE, uptime BIGINT",
        .logs => "timestamp TIMESTAMP, source VARCHAR, unit VARCHAR, identifier VARCHAR, systemd_unit VARCHAR, priority INTEGER, message TEXT, pid INTEGER",
    };
    return std.fmt.allocPrintSentinel(allocator, "CREATE TEMP TABLE {s}_staging ({s})", .{ table.name(), cols }, 0);
}

/// CREATE VIEW <table> AS read_parquet(glob) UNION ALL <table>_staging.
/// When no parquet exists yet the view is just the staging temp table, so the
/// empty case resolves without read_parquet erroring on a zero-file glob.
///
/// We SELECT the explicit column list (not `*`) from the parquet side because
/// `hive_partitioning=true` appends synthetic `date`/`hour` partition columns,
/// which would otherwise make the parquet branch wider than the staging branch
/// and break the UNION ALL's column-count check. The explicit list also keeps
/// column ordering identical across both branches and the row readers.
fn createView(allocator: Allocator, pq: *ParquetQuery, root_dir: []const u8, table: Table, has_parquet: bool) !void {
    const temp_name = table.name();
    const cols = columnList(table);
    const sql = if (has_parquet) blk: {
        const glob = try std.fmt.allocPrint(allocator, "{s}/{s}/**/*.parquet", .{ root_dir, table.name() });
        defer allocator.free(glob);
        break :blk try std.fmt.allocPrintSentinel(
            allocator,
            "CREATE VIEW {s} AS SELECT {s} FROM read_parquet('{s}', hive_partitioning=true, union_by_name=true) UNION ALL SELECT {s} FROM {s}_staging",
            .{ table.name(), cols, glob, cols, temp_name },
            0,
        );
    } else try std.fmt.allocPrintSentinel(
        allocator,
        "CREATE VIEW {s} AS SELECT {s} FROM {s}_staging",
        .{ table.name(), cols, temp_name },
        0,
    );
    defer allocator.free(sql);

    var result: c.duckdb_result = undefined;
    if (c.duckdb_query(pq.conn, sql.ptr, &result) == c.DuckDBError) {
        std.log.err("create view {s} error: {s}", .{ table.name(), c.duckdb_result_error(&result) });
        c.duckdb_destroy_result(&result);
        return QueryError.SchemaError;
    }
    c.duckdb_destroy_result(&result);
}

// ============================================================================
// Row readers (mirror storage.zig column ordering)
// ============================================================================

fn readMetricsRow(result: *c.duckdb_result, i: usize) collector.SystemMetrics {
    return .{
        .cpu_percent = c.duckdb_value_float(result, 1, i),
        .cpu_user = c.duckdb_value_float(result, 2, i),
        .cpu_system = c.duckdb_value_float(result, 3, i),
        .cpu_iowait = c.duckdb_value_float(result, 4, i),
        .mem_total = c.duckdb_value_uint64(result, 5, i),
        .mem_used = c.duckdb_value_uint64(result, 6, i),
        .mem_percent = c.duckdb_value_float(result, 7, i),
        .swap_total = c.duckdb_value_uint64(result, 8, i),
        .swap_used = c.duckdb_value_uint64(result, 9, i),
    };
}

fn readProcessRow(a: Allocator, result: *c.duckdb_result, i: usize) !collector.ProcessInfo {
    const pid = c.duckdb_value_uint32(result, 1, i);
    const name = try a.dupe(u8, std.mem.span(c.duckdb_value_varchar(result, 2, i)));
    errdefer a.free(name);
    const cmdline = try a.dupe(u8, std.mem.span(c.duckdb_value_varchar(result, 3, i)));
    errdefer a.free(cmdline);
    const state_char = std.mem.span(c.duckdb_value_varchar(result, 4, i))[0];
    const cpu_percent = c.duckdb_value_float(result, 5, i);
    const mem_rss = c.duckdb_value_uint64(result, 6, i);
    const threads = c.duckdb_value_uint32(result, 7, i);
    const username = try a.dupe(u8, std.mem.span(c.duckdb_value_varchar(result, 8, i)));
    errdefer a.free(username);
    const io_read_bytes = if (c.duckdb_value_is_null(result, 9, i)) 0 else c.duckdb_value_uint64(result, 9, i);
    const io_write_bytes = if (c.duckdb_value_is_null(result, 10, i)) 0 else c.duckdb_value_uint64(result, 10, i);
    const cgroup = if (c.duckdb_value_is_null(result, 11, i))
        try a.dupe(u8, "")
    else
        try a.dupe(u8, std.mem.span(c.duckdb_value_varchar(result, 11, i)));
    errdefer a.free(cgroup);
    const unit = if (c.duckdb_value_is_null(result, 12, i))
        try a.dupe(u8, "")
    else
        try a.dupe(u8, std.mem.span(c.duckdb_value_varchar(result, 12, i)));
    errdefer a.free(unit);
    return .{
        .pid = pid,
        .name = name,
        .cmdline = cmdline,
        .state = state_char,
        .cpu_percent = cpu_percent,
        .mem_rss = mem_rss,
        .threads = threads,
        .username = username,
        .io_read_bytes = io_read_bytes,
        .io_write_bytes = io_write_bytes,
        .cgroup = cgroup,
        .unit = unit,
    };
}

fn readDiskRow(a: Allocator, result: *c.duckdb_result, i: usize) !collector.DiskInfo {
    const mount_point = try a.dupe(u8, std.mem.span(c.duckdb_value_varchar(result, 1, i)));
    errdefer a.free(mount_point);
    const filesystem = try a.dupe(u8, std.mem.span(c.duckdb_value_varchar(result, 2, i)));
    errdefer a.free(filesystem);
    return .{
        .mount_point = mount_point,
        .filesystem = filesystem,
        .total_bytes = c.duckdb_value_uint64(result, 3, i),
        .used_bytes = c.duckdb_value_uint64(result, 4, i),
        .percent = c.duckdb_value_float(result, 5, i),
    };
}

fn readLogRow(a: Allocator, result: *c.duckdb_result, i: usize) !logs.LogEntry {
    const ts_struct = c.duckdb_value_timestamp(result, 0, i);
    const timestamp = @divTrunc(ts_struct.micros, 1_000_000);
    const source = try a.dupe(u8, std.mem.span(c.duckdb_value_varchar(result, 1, i)));
    errdefer a.free(source);
    const unit = if (c.duckdb_value_is_null(result, 2, i)) null else try a.dupe(u8, std.mem.span(c.duckdb_value_varchar(result, 2, i)));
    errdefer if (unit) |u| a.free(u);
    const identifier = if (c.duckdb_value_is_null(result, 3, i)) null else try a.dupe(u8, std.mem.span(c.duckdb_value_varchar(result, 3, i)));
    errdefer if (identifier) |id| a.free(id);
    const systemd_unit = if (c.duckdb_value_is_null(result, 4, i)) null else try a.dupe(u8, std.mem.span(c.duckdb_value_varchar(result, 4, i)));
    errdefer if (systemd_unit) |su| a.free(su);
    const priority: u8 = @intCast(c.duckdb_value_int32(result, 5, i));
    const message = try a.dupe(u8, std.mem.span(c.duckdb_value_varchar(result, 6, i)));
    errdefer a.free(message);
    const pid = if (c.duckdb_value_is_null(result, 7, i)) null else c.duckdb_value_uint32(result, 7, i);
    return .{
        .timestamp = timestamp,
        .source = source,
        .unit = unit,
        .identifier = identifier,
        .systemd_unit = systemd_unit,
        .priority = priority,
        .message = message,
        .pid = pid,
    };
}

fn freeProcess(a: Allocator, p: *collector.ProcessInfo) void {
    a.free(p.name);
    a.free(p.cmdline);
    a.free(p.username);
    a.free(p.cgroup);
    a.free(p.unit);
}

fn freeDisk(a: Allocator, d: *collector.DiskInfo) void {
    a.free(d.mount_point);
    a.free(d.filesystem);
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

fn testRoot(allocator: Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    return tmp.dir.realpathAlloc(allocator, ".");
}

fn sampleMetrics(cpu: f32) collector.SystemMetrics {
    return .{
        .cpu_percent = cpu,
        .cpu_user = cpu / 2,
        .cpu_system = cpu / 4,
        .cpu_iowait = 0.0,
        .mem_total = 8_000_000_000,
        .mem_used = 4_000_000_000,
        .mem_percent = 50.0,
        .swap_total = 0,
        .swap_used = 0,
    };
}

test "parquet_query: empty store resolves to zero rows" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    // No staging, no parquet. The views must still resolve.
    var pq = try initParquetQuery(a, root);
    defer pq.deinit();

    var res = try pq.rawQuery("SELECT COUNT(*) AS n FROM metrics");
    defer res.deinit();
    try testing.expectEqualStrings("0", res.rows[0][0].?);

    const latest = try pq.getLatestMetrics();
    try testing.expect(latest == null);
}

test "parquet_query: staging-only rows are visible through the view" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    var stg = try staging.Staging.open(a, root);
    defer stg.deinit();
    try stg.appendMetrics(1_700_000_000, sampleMetrics(10.0));
    try stg.appendMetrics(1_700_000_010, sampleMetrics(20.0));
    try stg.sync();

    var pq = try initParquetQuery(a, root);
    defer pq.deinit();

    var res = try pq.rawQuery("SELECT COUNT(*) AS n FROM metrics");
    defer res.deinit();
    try testing.expectEqualStrings("2", res.rows[0][0].?);
}

test "parquet_query: union of rolled parquet + un-rolled staging, no dup/miss" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    var stg = try staging.Staging.open(a, root);
    defer stg.deinit();

    // First batch -> roll to parquet.
    var i: i64 = 0;
    while (i < 4) : (i += 1) try stg.appendMetrics(1_700_000_000 + i * 10, sampleMetrics(@floatFromInt(i)));
    try stg.sync();
    const res = (try roll.rollTable(a, root, &stg, .metrics)).?;
    a.free(res.parquet_path);

    // Second batch -> leave only in staging (not rolled).
    var j: i64 = 0;
    while (j < 3) : (j += 1) try stg.appendMetrics(1_700_001_000 + j * 10, sampleMetrics(@floatFromInt(100 + j)));
    try stg.sync();

    var pq = try initParquetQuery(a, root);
    defer pq.deinit();

    // The view must return the FULL set: 4 (parquet) + 3 (staging) = 7.
    var res2 = try pq.rawQuery("SELECT COUNT(*) AS n FROM metrics");
    defer res2.deinit();
    try testing.expectEqualStrings("7", res2.rows[0][0].?);

    // No duplicates: distinct timestamps must equal the row count.
    var res3 = try pq.rawQuery("SELECT COUNT(DISTINCT timestamp) AS n FROM metrics");
    defer res3.deinit();
    try testing.expectEqualStrings("7", res3.rows[0][0].?);

    // getMetricsRange spans both tiers and returns all 7 in order.
    const range = try pq.getMetricsRange(1_700_000_000, 1_700_002_000);
    defer a.free(range);
    try testing.expectEqual(@as(usize, 7), range.len);
}

test "parquet_query: typed getters over union (processes + logs)" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    var stg = try staging.Staging.open(a, root);
    defer stg.deinit();

    // Rolled process row.
    const procs1 = [_]collector.ProcessInfo{.{
        .pid = 1,
        .name = "rolled",
        .cmdline = "rolled",
        .state = 'S',
        .cpu_percent = 5.0,
        .mem_rss = 1000,
        .threads = 1,
        .username = "root",
        .io_read_bytes = 0,
        .io_write_bytes = 0,
        .cgroup = "",
        .unit = "",
    }};
    try stg.appendProcesses(1_700_000_000, &procs1);
    try stg.sync();
    const pres = (try roll.rollTable(a, root, &stg, .processes)).?;
    a.free(pres.parquet_path);

    // Staging-only process row at a LATER timestamp (so it's the MAX).
    const procs2 = [_]collector.ProcessInfo{.{
        .pid = 2,
        .name = "staged",
        .cmdline = "staged",
        .state = 'R',
        .cpu_percent = 9.0,
        .mem_rss = 2000,
        .threads = 2,
        .username = "dmeh",
        .io_read_bytes = 7,
        .io_write_bytes = 8,
        .cgroup = "/x",
        .unit = "x.service",
    }};
    try stg.appendProcesses(1_700_001_000, &procs2);
    try stg.sync();

    // Logs across both tiers.
    const logs1 = [_]logs.LogEntry{.{ .timestamp = 1_700_000_000, .source = "systemd", .unit = "u", .identifier = null, .systemd_unit = "u.service", .priority = 6, .message = "rolled log", .pid = null }};
    try stg.appendLogs(&logs1);
    try stg.sync();
    const lres = (try roll.rollTable(a, root, &stg, .logs)).?;
    a.free(lres.parquet_path);
    const logs2 = [_]logs.LogEntry{.{ .timestamp = 1_700_001_000, .source = "file", .unit = null, .identifier = null, .systemd_unit = null, .priority = 3, .message = "staged log", .pid = 99 }};
    try stg.appendLogs(&logs2);
    try stg.sync();

    var pq = try initParquetQuery(a, root);
    defer pq.deinit();

    // getProcesses(null) returns the latest-timestamp row (staging-only).
    const latest_procs = try pq.getProcesses(null);
    defer {
        for (latest_procs) |*p| freeProcess(a, p);
        a.free(latest_procs);
    }
    try testing.expectEqual(@as(usize, 1), latest_procs.len);
    try testing.expectEqualStrings("staged", latest_procs[0].name);
    try testing.expectEqual(@as(u32, 2), latest_procs[0].pid);

    // queryLogs returns both tiers' entries.
    const all_logs = try pq.queryLogs(null, null, null);
    defer {
        for (all_logs) |*e| e.deinit(a);
        a.free(all_logs);
    }
    try testing.expectEqual(@as(usize, 2), all_logs.len);
}
