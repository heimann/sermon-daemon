//! On-demand query path for the parquet hot tier (plan 25).
//!
//! Queries are rare and human-triggered (sermon query, the stdio MCP). Each one
//! brings DuckDB up TRANSIENTLY in-memory, builds a per-table compatibility
//! VIEW named exactly like today's tables (metrics/processes/disks/containers/
//! logs), runs the SQL, and exits. DuckDB is never resident.
//!
//! Each view is:
//!
//!     read_parquet(['<f1>','<f2>',...], hive_partitioning=false, union_by_name=true)
//!       UNION ALL <table>_staging
//!
//! where the file list is an EXPLICIT, SQL-escaped set of parquet paths FROZEN
//! at init under the lock (NOT a lazy glob - DuckDB would re-evaluate a glob at
//! query time, after the lock is released, and a roll in that window could make
//! the glob double-count against the still-frozen staging temp). `<table>_staging`
//! is a TEMP table loaded from a Zig-decoded staging snapshot (the rows not yet
//! rolled to parquet).
//!
//! NO DOUBLE-COUNT, NO MISS (ACROSS CRASHES AND LIVE INTERLEAVINGS):
//!   The guarantee holds in two layers:
//!
//!   1. ACROSS CRASHES (on-disk ordering): the roll writes the parquet to a TEMP
//!      `<final>.tmp`, fsyncs it, THEN resets (truncates + fsyncs) staging, and
//!      ONLY THEN renames the temp into its final `*.parquet` name. So a committed
//!      parquet implies staging was ALREADY cleared - there is never a persisted
//!      state with the same rows in both a committed parquet and staging. A crash
//!      between reset and rename leaves staging empty + a durable orphan `.tmp`
//!      (a transient, recoverable miss); roll.recoverOrphanTemps renames such
//!      orphans on daemon startup. collectParquetFiles matches only `.parquet`,
//!      never the in-flight `.parquet.tmp`.
//!
//!   2. LIVE INTERLEAVINGS (advisory lock): ordering alone does not stop a query
//!      that, mid-roll, enumerates a just-renamed parquet AND snapshots a staging
//!      not yet observed-as-reset from counting rows twice. A cross-process
//!      advisory lock on `<root>/_staging/.roll.lock` (std.posix.flock) closes
//!      that window:
//!        - the roll holds LOCK_EX across its whole temp-write + fsync + reset +
//!          rename critical section (see roll.rollTable);
//!        - a query holds LOCK_SH across BOTH its parquet enumeration AND its
//!          staging snapshot (see initParquetQuery), so it can never observe the
//!          published-but-not-yet-reset state.
//!
//!   A row is therefore in exactly one branch: either staging (not yet rolled)
//!   or parquet (rolled AND already cleared from staging), never both.
//!
//!   We also keep the enumerate-parquet-before-snapshot-staging ordering as
//!   defense in depth: even were the lock absent, missing a brand-new parquet
//!   file leaves its rows in the older staging snapshot, counted exactly once.
//!
//! DAY COMPACTION shares this same crash contract. roll.compactDay deletes a
//! sealed day's input parquet BEFORE publishing the merged `<seq>.parquet`, all
//! under LOCK_EX - so a LIVE query (LOCK_SH) can never observe that window, just
//! as with the roll. A CRASH mid-compaction can leave a committed manifest with
//! inputs deleted and the merge not yet published (rows durable in `<seq>
//! .parquet.building`, which this read path ignores) - the SAME transient,
//! recoverable miss the roll's `.tmp` orphan creates. roll.recoverCompactions
//! repairs it at daemon STARTUP under LOCK_EX (right after recoverOrphanTemps),
//! exactly as recoverOrphanTemps repairs a roll's orphan. The read path stays
//! manifest-agnostic by design: a query run against a crashed-daemon tree before
//! that startup recovery completes sees the same transient miss the roll already
//! accepted, never a double-count.
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
///
/// FRESHNESS CONTRACT: a handle snapshots staging ONCE at init (the un-rolled
/// rows are copied into TEMP tables then) and resolves the parquet glob once.
/// It is single-query / open-and-exit: reusing a handle across calls serves
/// stale staging data and misses any parquet rolled after init. Open a fresh
/// handle per logical query.
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
                    // duckdb_value_varchar mallocs; dupeVarchar frees the original.
                    rows[row_idx][col_idx] = try dupeVarchar(self.allocator, c.duckdb_value_varchar(&result, col_idx, row_idx));
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
        // Check each bind: a failure here (wrong index/type) would otherwise
        // silently leave a parameter unset and skew the result.
        if (c.duckdb_bind_int64(stmt, 1, since) == c.DuckDBError) return QueryError.QueryError;
        if (c.duckdb_bind_int64(stmt, 2, until) == c.DuckDBError) return QueryError.QueryError;

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
            if (c.duckdb_bind_timestamp(stmt, 1, .{ .micros = tsMicrosSaturating(ts) }) == c.DuckDBError) return QueryError.QueryError;
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
            if (c.duckdb_bind_timestamp(stmt, 1, .{ .micros = tsMicrosSaturating(ts) }) == c.DuckDBError) return QueryError.QueryError;
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

        const b1 = if (since) |s| c.duckdb_bind_int64(stmt, 1, s) else c.duckdb_bind_null(stmt, 1);
        if (b1 == c.DuckDBError) return QueryError.QueryError;
        const b2 = if (unit) |u| c.duckdb_bind_varchar_length(stmt, 2, u.ptr, u.len) else c.duckdb_bind_null(stmt, 2);
        if (b2 == c.DuckDBError) return QueryError.QueryError;
        const b3 = if (priority) |p| c.duckdb_bind_uint8(stmt, 3, p) else c.duckdb_bind_null(stmt, 3);
        if (b3 == c.DuckDBError) return QueryError.QueryError;

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
///
/// FRESHNESS CONTRACT: this snapshots staging once, here. See ParquetQuery's
/// doc-comment - the returned handle is single-query / open-and-exit.
pub fn initParquetQuery(allocator: Allocator, root_dir: []const u8) !ParquetQuery {
    var db: c.duckdb_database = undefined;
    if (c.duckdb_open(":memory:", &db) == c.DuckDBError) return QueryError.DatabaseError;
    errdefer c.duckdb_close(&db);

    var conn: c.duckdb_connection = undefined;
    if (c.duckdb_connect(db, &conn) == c.DuckDBError) return QueryError.ConnectionError;
    errdefer c.duckdb_disconnect(&conn);

    var pq = ParquetQuery{ .allocator = allocator, .db = db, .conn = conn };

    // Hold the roll lock SHARED across the WHOLE enumerate-parquet + snapshot-
    // staging loop. The roll takes it EXCLUSIVE around its whole temp-write +
    // fsync + reset + rename critical section, so while we hold it shared no roll
    // can move rows from staging to parquet underneath us - we never observe the
    // published-but-not-yet-reset window that would double-count. See the module-
    // level comment. The enumerate-before-snapshot ordering below stays as defense
    // in depth.
    var lock_file = try staging.openRollLock(root_dir, allocator);
    defer lock_file.close();
    try std.posix.flock(lock_file.handle, std.posix.LOCK.SH);
    defer std.posix.flock(lock_file.handle, std.posix.LOCK.UN) catch {};

    for (Table.all) |table| {
        // ORDER MATTERS: MATERIALIZE the parquet file-set first (a frozen,
        // explicit list of file paths), THEN snapshot staging - both under the
        // SH lock. Freezing the list (rather than a lazy glob the view re-
        // evaluates at query time) is load-bearing: a roll between this init and
        // the later user query cannot change what this handle sees, so a row
        // rolled after init stays counted exactly once via the still-frozen
        // staging temp. See the module-level comment.
        var files = try collectParquetFiles(allocator, root_dir, table);
        defer {
            for (files.items) |p| allocator.free(p);
            files.deinit(allocator);
        }

        try createStagingTemp(allocator, &pq, root_dir, table);
        try createView(allocator, &pq, table, files.items);
    }

    return pq;
}

/// Collect the absolute paths of every `.parquet` file for this table into an
/// owned list (caller frees each path + the list). An empty list (no directory
/// or no files) yields a staging-only view. The returned list is FROZEN here
/// under the SH lock so the view cannot re-glob and see a post-init roll.
fn collectParquetFiles(allocator: Allocator, root_dir: []const u8, table: Table) !std.ArrayList([]u8) {
    var files = std.ArrayList([]u8){};
    errdefer {
        for (files.items) |p| allocator.free(p);
        files.deinit(allocator);
    }

    const dir_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ root_dir, table.name() });
    defer allocator.free(dir_path);

    var dir = fs.openDirAbsolute(dir_path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return files,
        else => return err,
    };
    defer dir.close();

    var walker = try dir.walk(allocator);
    defer walker.deinit();
    while (try walker.next()) |entry| {
        // Match ONLY committed files ending in exactly `.parquet`; a roll's in-
        // flight `<seq>.parquet.tmp` does NOT end in `.parquet`, so it is excluded
        // here (it must never join the frozen file set - its rows are still in
        // staging until the rename publishes it).
        if (entry.kind == .file and std.mem.endsWith(u8, entry.basename, ".parquet")) {
            // entry.path is relative to dir_path; build the absolute path.
            const abs = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ dir_path, entry.path });
            errdefer allocator.free(abs);
            try files.append(allocator, abs);
        }
    }
    return files;
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
        .container_metrics => "timestamp, vmid, cpu_pct, mem_current, mem_max",
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
        .container_metrics => "timestamp TIMESTAMP, vmid INTEGER, cpu_pct DOUBLE, mem_current BIGINT, mem_max BIGINT",
    };
    return std.fmt.allocPrintSentinel(allocator, "CREATE TEMP TABLE {s}_staging ({s})", .{ table.name(), cols }, 0);
}

/// CREATE VIEW <table> AS read_parquet([<frozen file list>]) UNION ALL
/// <table>_staging. When `files` is empty the view is just the staging temp
/// table, so the empty case resolves without read_parquet erroring on a zero-
/// file list.
///
/// The parquet side reads an EXPLICIT, SQL-escaped list of file paths frozen at
/// init (NOT a lazy glob): DuckDB re-evaluates a glob at query time, after the
/// SH lock is released, so a roll between init and the user query could make the
/// glob see a new parquet while the staging temp still holds the same rows -
/// double-counting. The frozen list pins exactly the files that existed under
/// the lock. Every path is double-quote-escaped (escapeSqlLiteral) so a path
/// containing a single quote can neither break the literal nor inject.
///
/// We SELECT the explicit column list (not `*`) from the parquet side so the
/// parquet branch's column set matches the staging branch and the row readers.
///
/// hive_partitioning is DISABLED. Day-compaction (roll.compactDay) merges a
/// sealed day's hour-partitioned files into a single `date=X/<seq>.parquet` at
/// the DAY level, so the tree holds MIXED partition depths: un-compacted days
/// at `date=X/hour=Y/...` (date+hour keys) and compacted days at `date=X/...`
/// (date key only). DuckDB's `hive_partitioning=true` errors on that
/// inconsistent depth within one read_parquet call. We never use the hive
/// date/hour keys anyway - queries filter on the real `timestamp` data column,
/// retention parses the date from the directory NAME, not via hive - so turning
/// hive off lets the mixed-depth file set read uniformly. `union_by_name=true`
/// stays on so schema differences across files are reconciled by column name.
fn createView(allocator: Allocator, pq: *ParquetQuery, table: Table, files: []const []const u8) !void {
    const temp_name = table.name();
    const cols = columnList(table);

    const sql = if (files.len > 0) blk: {
        const file_list = try buildFileListLiteral(allocator, files);
        defer allocator.free(file_list);
        break :blk try std.fmt.allocPrintSentinel(
            allocator,
            "CREATE VIEW {s} AS SELECT {s} FROM read_parquet([{s}], hive_partitioning=false, union_by_name=true) UNION ALL SELECT {s} FROM {s}_staging",
            .{ table.name(), cols, file_list, cols, temp_name },
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

/// Build a comma-separated list of single-quoted, SQL-escaped file paths for the
/// read_parquet([...]) literal: `'<f1>','<f2>',...`. Each path is run through
/// roll.escapeSqlLiteral so an embedded single quote is doubled. Caller frees.
fn buildFileListLiteral(allocator: Allocator, files: []const []const u8) ![]u8 {
    var out = std.ArrayList(u8){};
    errdefer out.deinit(allocator);
    for (files, 0..) |path, idx| {
        if (idx != 0) try out.append(allocator, ',');
        const escaped = try roll.escapeSqlLiteral(allocator, path);
        defer allocator.free(escaped);
        try out.append(allocator, '\'');
        try out.appendSlice(allocator, escaped);
        try out.append(allocator, '\'');
    }
    return out.toOwnedSlice(allocator);
}

// ============================================================================
// Row readers (mirror storage.zig column ordering)
// ============================================================================

/// duckdb_value_varchar mallocs the returned C string; dupe it into our
/// allocator and duckdb_free the original so it does not leak. A null result
/// (NULL cell or OOM inside DuckDB) dupes the empty string.
fn dupeVarchar(a: Allocator, ptr: [*c]u8) ![]u8 {
    if (ptr == null) return a.dupe(u8, "");
    defer c.duckdb_free(ptr);
    return a.dupe(u8, std.mem.span(ptr));
}

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
    const name = try dupeVarchar(a, c.duckdb_value_varchar(result, 2, i));
    errdefer a.free(name);
    const cmdline = try dupeVarchar(a, c.duckdb_value_varchar(result, 3, i));
    errdefer a.free(cmdline);
    // A tampered/empty parquet could yield a zero-length state string; indexing
    // [0] would be out of bounds. Default to '?' when empty.
    const state_str = try dupeVarchar(a, c.duckdb_value_varchar(result, 4, i));
    defer a.free(state_str);
    const state_char: u8 = if (state_str.len == 0) '?' else state_str[0];
    const cpu_percent = c.duckdb_value_float(result, 5, i);
    const mem_rss = c.duckdb_value_uint64(result, 6, i);
    const threads = c.duckdb_value_uint32(result, 7, i);
    const username = try dupeVarchar(a, c.duckdb_value_varchar(result, 8, i));
    errdefer a.free(username);
    const io_read_bytes = if (c.duckdb_value_is_null(result, 9, i)) 0 else c.duckdb_value_uint64(result, 9, i);
    const io_write_bytes = if (c.duckdb_value_is_null(result, 10, i)) 0 else c.duckdb_value_uint64(result, 10, i);
    const cgroup = if (c.duckdb_value_is_null(result, 11, i))
        try a.dupe(u8, "")
    else
        try dupeVarchar(a, c.duckdb_value_varchar(result, 11, i));
    errdefer a.free(cgroup);
    const unit = if (c.duckdb_value_is_null(result, 12, i))
        try a.dupe(u8, "")
    else
        try dupeVarchar(a, c.duckdb_value_varchar(result, 12, i));
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
    const mount_point = try dupeVarchar(a, c.duckdb_value_varchar(result, 1, i));
    errdefer a.free(mount_point);
    const filesystem = try dupeVarchar(a, c.duckdb_value_varchar(result, 2, i));
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
    const source = try dupeVarchar(a, c.duckdb_value_varchar(result, 1, i));
    errdefer a.free(source);
    const unit = if (c.duckdb_value_is_null(result, 2, i)) null else try dupeVarchar(a, c.duckdb_value_varchar(result, 2, i));
    errdefer if (unit) |u| a.free(u);
    const identifier = if (c.duckdb_value_is_null(result, 3, i)) null else try dupeVarchar(a, c.duckdb_value_varchar(result, 3, i));
    errdefer if (identifier) |id| a.free(id);
    const systemd_unit = if (c.duckdb_value_is_null(result, 4, i)) null else try dupeVarchar(a, c.duckdb_value_varchar(result, 4, i));
    errdefer if (systemd_unit) |su| a.free(su);
    // priority is stored as INTEGER; a tampered parquet could hold a value
    // outside 0-255 that would panic/UB on a bare @intCast to u8. Clamp first.
    const priority: u8 = @intCast(std.math.clamp(c.duckdb_value_int32(result, 5, i), 0, 255));
    const message = try dupeVarchar(a, c.duckdb_value_varchar(result, 6, i));
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

// Reuse staging's owned-string free helpers (same struct types) to avoid a
// second copy that could drift from collector.ProcessInfo/DiskInfo's fields.
const freeProcess = staging.freeProcess;
const freeDisk = staging.freeDisk;

/// Seconds -> micros for a TIMESTAMP bind, saturating instead of overflowing
/// i64. The bound value can originate from an absurd caller/decoded timestamp,
/// and a bare `* 1_000_000` would panic/wrap; a clamped bound just selects no
/// rows, which is the correct behavior for an out-of-range filter.
fn tsMicrosSaturating(ts: i64) i64 {
    const micros: i64 = std.math.mul(i64, ts, 1_000_000) catch
        if (ts < 0) std.math.minInt(i64) else std.math.maxInt(i64);
    return micros;
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

test "parquet_query: frozen file list - a roll after init does not change a handle's view (no double-count)" {
    // Fix 1: the parquet file set is materialized (frozen) at init under the SH
    // lock, NOT a lazy glob. After init we move the staging rows to parquet via a
    // roll; an already-open handle must NOT see the new parquet file (its list is
    // frozen) and must NOT double-count. A glob-based view would re-evaluate at
    // query time and count the rolled rows in BOTH branches.
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    var stg = try staging.Staging.open(a, root);
    defer stg.deinit();

    // 3 rows in staging, no parquet yet.
    var i: i64 = 0;
    while (i < 3) : (i += 1) try stg.appendMetrics(1_700_000_000 + i * 10, sampleMetrics(@floatFromInt(i)));
    try stg.sync();

    // Open the handle: it freezes an EMPTY parquet list and snapshots 3 staging rows.
    var pq = try initParquetQuery(a, root);
    defer pq.deinit();

    // Now roll: staging -> parquet (and staging reset). This happens AFTER init.
    const res = (try roll.rollTable(a, root, &stg, .metrics)).?;
    a.free(res.parquet_path);

    // The already-open handle still sees exactly 3 (its frozen staging snapshot),
    // NOT 6: the post-init parquet is invisible to the frozen file list, so no
    // double-count. A fresh handle would see the 3 rolled rows from parquet.
    var res2 = try pq.rawQuery("SELECT COUNT(*) AS n FROM metrics");
    defer res2.deinit();
    try testing.expectEqualStrings("3", res2.rows[0][0].?);

    var pq2 = try initParquetQuery(a, root);
    defer pq2.deinit();
    var res3 = try pq2.rawQuery("SELECT COUNT(*) AS n FROM metrics");
    defer res3.deinit();
    try testing.expectEqualStrings("3", res3.rows[0][0].?);
}

test "parquet_query: a parquet path containing a single quote is escaped, not broken" {
    // Fix 3: createView SQL-escapes every interpolated parquet path. Place a real
    // parquet file under a directory whose name contains a single quote and prove
    // the view both builds (no SQL break/injection) and reads the rows.
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    // Produce a real parquet via a normal roll, then relocate it under a
    // single-quote directory inside the table's tree so collectParquetFiles
    // finds it and createView must escape the path.
    var stg = try staging.Staging.open(a, root);
    defer stg.deinit();
    try stg.appendMetrics(1_700_000_000, sampleMetrics(42.0));
    try stg.sync();
    const res = (try roll.rollTable(a, root, &stg, .metrics)).?;
    defer a.free(res.parquet_path);

    // metrics dir exists now; create a quoted subdir and move the parquet in.
    const quoted_dir = try std.fmt.allocPrint(a, "{s}/metrics/o'clock", .{root});
    defer a.free(quoted_dir);
    try fs.makeDirAbsolute(quoted_dir);
    const dest = try std.fmt.allocPrint(a, "{s}/seg.parquet", .{quoted_dir});
    defer a.free(dest);
    try fs.renameAbsolute(res.parquet_path, dest);

    var pq = try initParquetQuery(a, root);
    defer pq.deinit();
    var cnt = try pq.rawQuery("SELECT COUNT(*) AS n FROM metrics");
    defer cnt.deinit();
    try testing.expectEqualStrings("1", cnt.rows[0][0].?);
}

test "parquet_query: collectParquetFiles ignores a *.parquet.tmp in the tree" {
    // Crash-consistency fix: an in-flight roll's `<seq>.parquet.tmp` must never
    // join the frozen file set (its rows are still in staging until the rename
    // publishes it). Place one real `.parquet` plus a sibling `.parquet.tmp`
    // (a copy of the same file) and assert the view counts the committed file
    // exactly once - never twice.
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    var stg = try staging.Staging.open(a, root);
    defer stg.deinit();
    try stg.appendMetrics(1_700_000_000, sampleMetrics(7.0));
    try stg.sync();
    const res = (try roll.rollTable(a, root, &stg, .metrics)).?;
    defer a.free(res.parquet_path);

    // Drop a `.parquet.tmp` alongside the committed file (byte-for-byte copy).
    const tmp_sibling = try std.fmt.allocPrint(a, "{s}.tmp", .{res.parquet_path});
    defer a.free(tmp_sibling);
    try fs.copyFileAbsolute(res.parquet_path, tmp_sibling, .{});

    // collectParquetFiles must skip the .tmp: exactly one committed file is seen.
    var files = try collectParquetFiles(a, root, .metrics);
    defer {
        for (files.items) |p| a.free(p);
        files.deinit(a);
    }
    try testing.expectEqual(@as(usize, 1), files.items.len);
    try testing.expect(std.mem.endsWith(u8, files.items[0], ".parquet"));

    // And the view counts the single committed row, not the .tmp's copy too.
    var pq = try initParquetQuery(a, root);
    defer pq.deinit();
    var cnt = try pq.rawQuery("SELECT COUNT(*) AS n FROM metrics");
    defer cnt.deinit();
    try testing.expectEqualStrings("1", cnt.rows[0][0].?);
}

test "parquet_query: container_metrics union of rolled parquet + staging" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    var stg = try staging.Staging.open(a, root);
    defer stg.deinit();

    // Roll one batch of CT metrics to parquet.
    const rolled = [_]proxmox.ContainerMetrics{
        .{ .vmid = 101, .cpu_pct = 10.0, .mem_current = 1_000_000, .mem_max = 4_000_000 },
        .{ .vmid = 102, .cpu_pct = 20.0, .mem_current = 2_000_000, .mem_max = null },
    };
    try stg.appendContainerMetrics(1_700_000_000, &rolled);
    try stg.sync();
    const res = (try roll.rollTable(a, root, &stg, .container_metrics)).?;
    a.free(res.parquet_path);

    // Leave a second batch (including a NaN sentinel row) only in staging.
    const staged = [_]proxmox.ContainerMetrics{
        .{ .vmid = 103, .cpu_pct = std.math.nan(f64), .mem_current = 3_000_000, .mem_max = null },
    };
    try stg.appendContainerMetrics(1_700_001_000, &staged);
    try stg.sync();

    var pq = try initParquetQuery(a, root);
    defer pq.deinit();

    // 2 (parquet) + 1 (staging) = 3 rows, no dup/miss across the UNION.
    var cnt = try pq.rawQuery("SELECT COUNT(*) AS n FROM container_metrics");
    defer cnt.deinit();
    try testing.expectEqualStrings("3", cnt.rows[0][0].?);

    // The NaN cpu_pct staging row reads back as SQL NULL through the view.
    var nulls = try pq.rawQuery("SELECT COUNT(*) AS n FROM container_metrics WHERE cpu_pct IS NULL");
    defer nulls.deinit();
    try testing.expectEqualStrings("1", nulls.rows[0][0].?);
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

test "parquet_query: mixed-depth tree (day-merged + hour-partitioned) reads each row once" {
    // The hive-depth fix: one sealed day is compacted to date=X/<seq>.parquet
    // (date key only), while another stays hour-partitioned at date=Y/hour=Z/...
    // (date+hour keys). With hive_partitioning=false the mixed-depth file set
    // reads cleanly and every row is returned exactly once - no miss, no dup.
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    // Helper: roll one metrics row at `ts` then relocate the file into
    // <root>/metrics/date=<date>/hour=<hh>/ so we control the leaf layout.
    const Seed = struct {
        fn one(alloc: Allocator, r: []const u8, ts: i64, date: []const u8, hh: usize) !void {
            var stg = try staging.Staging.open(alloc, r);
            defer stg.deinit();
            try stg.appendMetrics(ts, sampleMetrics(1.0));
            try stg.sync();
            const res = (try roll.rollTable(alloc, r, &stg, .metrics)).?;
            defer alloc.free(res.parquet_path);
            const hour_dir = try std.fmt.allocPrint(alloc, "{s}/metrics/date={s}/hour={d:0>2}", .{ r, date, hh });
            defer alloc.free(hour_dir);
            try makeDirsForTest(hour_dir);
            const base = std.fs.path.basename(res.parquet_path);
            const dest = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ hour_dir, base });
            defer alloc.free(dest);
            try fs.renameAbsolute(res.parquet_path, dest);
        }
    };

    // Day A (2023-11-14): 3 hour leaves, one row each, then compact to a day file.
    try Seed.one(a, root, 1_700_000_000, "2023-11-14", 0);
    try Seed.one(a, root, 1_700_000_100, "2023-11-14", 1);
    try Seed.one(a, root, 1_700_000_200, "2023-11-14", 2);
    try roll.compactDay(a, root, .metrics, "2023-11-14");

    // Day B (2022-04-15): 2 hour leaves, one row each, left hour-partitioned.
    try Seed.one(a, root, 1_650_000_000, "2022-04-15", 0);
    try Seed.one(a, root, 1_650_000_100, "2022-04-15", 1);

    var pq = try initParquetQuery(a, root);
    defer pq.deinit();

    var cnt = try pq.rawQuery("SELECT COUNT(*) AS n FROM metrics");
    defer cnt.deinit();
    try testing.expectEqualStrings("5", cnt.rows[0][0].?); // 3 + 2, no miss

    var distinct = try pq.rawQuery("SELECT COUNT(DISTINCT timestamp) AS n FROM metrics");
    defer distinct.deinit();
    try testing.expectEqualStrings("5", distinct.rows[0][0].?); // no dup
}

/// Recursively create an absolute directory path for tests (mkdir -p). roll.zig
/// has an internal makePathAbsolute but it isn't pub; this small helper keeps the
/// test self-contained.
fn makeDirsForTest(path: []const u8) !void {
    var i: usize = 0;
    while (std.mem.indexOfScalarPos(u8, path, i + 1, '/')) |idx| {
        const prefix = path[0..idx];
        if (prefix.len != 0) fs.makeDirAbsolute(prefix) catch |e| switch (e) {
            error.PathAlreadyExists => {},
            else => return e,
        };
        i = idx;
    }
    fs.makeDirAbsolute(path) catch |e| switch (e) {
        error.PathAlreadyExists => {},
        else => return e,
    };
}
