//! The roll: the ONLY place parquet is produced (plan 25).
//!
//! At an N-min / M-MB trigger the daemon "rolls" a staging segment to a parquet
//! file. The roll decodes the durable segment, opens a SHORT-LIVED in-process
//! DuckDB (in-memory), CREATE TABLEs the schema, appender-inserts the decoded
//! rows, then `COPY ... TO '<hive>.tmp' (FORMAT parquet, COMPRESSION zstd)`,
//! closes every DuckDB handle and calls malloc_trim(0). DuckDB is never resident
//! - it lives only for the duration of one roll.
//!
//! CRASH-CONSISTENT RESET-BEFORE-PUBLISH: the parquet is written to a TEMP path
//! `<final>.tmp` and fsynced (file + dir) FIRST. Only once the temp is durable do
//! we reset (truncate + fsync) the staging segment, and only THEN do we rename
//! `<final>.tmp` -> `<final>` and fsync the directory. A file matching the
//! committed `*.parquet` name therefore exists ONLY AFTER staging was cleared, so
//! there is never an on-disk state with the SAME rows in both a committed parquet
//! AND staging - the double-count that plain publish-then-reset would leave after
//! a crash in its window. The worst case is a crash between reset and rename:
//! staging empty + a durable orphan `.tmp` not yet renamed = a transient,
//! recoverable MISS (the rows are durable in the .tmp) while the daemon is down,
//! repaired at startup by recoverOrphanTemps. If the temp's file fsync fails the
//! roll ABORTS before resetting staging, so staging is never cleared on un-durable
//! data. This guarantee holds ACROSS CRASHES; live interleavings against a query
//! are additionally covered by the EX/SH roll lock (see staging.zig).
//!
//! Idempotent replay: the parquet filename `<seq>` is derived from a hash of
//! the segment's raw bytes, NOT wall-clock. Re-rolling the same segment after a
//! crash (segment not yet reset) writes the SAME temp/final file, overwriting it,
//! so a crash before reset cannot double-count.
//!
//! Hive layout (warm-compatible, plans 15/22):
//!   <root>/<table>/date=YYYY-MM-DD/hour=HH/<seq>.parquet

const std = @import("std");
const Allocator = std.mem.Allocator;
const fs = std.fs;

const c = @cImport({
    @cInclude("duckdb.h");
});

extern fn malloc_trim(pad: usize) c_int;

const staging = @import("staging");
const collector = @import("collector");
const logs = @import("logs");
const proxmox = @import("proxmox");

const Table = staging.Table;
const Snapshot = staging.Snapshot;

pub const RollError = error{
    DatabaseError,
    ConnectionError,
    SchemaError,
    AppendError,
    CopyError,
    SyncError,
    OutOfMemory,
};

/// Result of a successful roll: the absolute parquet path written. Caller owns.
pub const RollResult = struct {
    parquet_path: []u8,
    row_count: usize,
};

/// Roll one table's staging segment to a parquet file, then truncate the
/// segment. Returns null when the segment has no rows (nothing to roll); in
/// that case the segment is left untouched. On success the staging segment is
/// reset and the written parquet path is returned (caller frees it).
///
/// `stg` is the live Staging handle (so reset reuses its open fd). The decode
/// reads the segment file from disk independently, which is safe: the append fd
/// is O_APPEND so a concurrent decode sees a consistent prefix.
pub fn rollTable(allocator: Allocator, root: []const u8, stg: *staging.Staging, table: Table) !?RollResult {
    // Take the EXCLUSIVE roll lock at the VERY START - BEFORE reading the
    // segment - and hold it across the WHOLE critical section: read+decode ->
    // writeParquet(COPY to .tmp) -> fsync temp -> stg.reset (truncate staging) ->
    // rename .tmp -> final -> fsync dir. An append (a collect cycle) takes the
    // same EX lock (staging.beginCycle/endCycle), so an append landing between our
    // read and our reset is impossible: it either completes entirely before we
    // read or entirely after we reset, never truncated and lost. A query holds
    // this lock SHARED around its enumerate-parquet + snapshot-staging (see
    // parquet_query.initParquetQuery); holding EX here keeps that query from
    // observing the published-but-not-yet-reset window (which would double-count
    // the rolled rows across both branches of the UNION ALL). See the CONCURRENCY
    // MODEL comment in staging.zig.
    var lock_file = try staging.openRollLock(root, allocator);
    defer lock_file.close();
    try std.posix.flock(lock_file.handle, std.posix.LOCK.EX);
    defer std.posix.flock(lock_file.handle, std.posix.LOCK.UN) catch {};

    // Read the segment once (UNDER the lock): hash the raw bytes for the
    // idempotent sequence id AND decode them into the snapshot, rather than
    // reading the file twice. The read is capped at staging.max_segment_size so a
    // crafted/huge on-disk file cannot force an unbounded allocation.
    const seg_path = try staging.segmentPath(allocator, root, table);
    defer allocator.free(seg_path);
    const data = blk: {
        const f = try fs.openFileAbsolute(seg_path, .{});
        defer f.close();
        const size = (try f.stat()).size;
        if (size > staging.max_segment_size) return staging.StagingError.CorruptSegment;
        break :blk try f.readToEndAlloc(allocator, staging.max_segment_size);
    };
    defer allocator.free(data);

    var snap = try staging.StagingReader.decodeSegment(allocator, table, data);
    defer snap.deinit();

    if (snap.rowCount() == 0) return null;

    // Derive the idempotent sequence id from the raw segment bytes so a
    // re-roll after a crash overwrites the same file.
    const seq = try seqFromBytes(allocator, data);
    defer allocator.free(seq);

    const partition_ts = firstTimestamp(snap) orelse std.time.timestamp();
    const parquet_path = try buildHivePath(allocator, root, table, partition_ts, seq);
    errdefer allocator.free(parquet_path);

    // Write to a TEMP path first so the committed `*.parquet` name never appears
    // on disk until staging has been reset (crash-consistency, see the module
    // comment). A re-roll overwrites the same temp/final names, so this is
    // idempotent on replay.
    const tmp_path = try std.fmt.allocPrint(allocator, "{s}.tmp", .{parquet_path});
    defer allocator.free(tmp_path);

    const rows_written = try writeParquet(allocator, table, &snap, tmp_path);

    // (a) Make the temp fully durable (file + dir). If the FILE fsync fails this
    // PROPAGATES and ABORTS the roll BEFORE we touch staging, so staging is never
    // cleared on un-durable data.
    try fsyncFileAndDir(tmp_path);

    // (b) Only now that the temp is durable do we clear the segment. A crash
    // before this leaves the segment intact and the next roll is idempotent.
    try stg.reset(table);

    // (c) Publish atomically: rename the durable temp onto its final name, then
    // fsync the directory so the new entry survives a crash. A crash between (b)
    // and (c) leaves staging empty + an orphan `.tmp`, recovered at startup by
    // recoverOrphanTemps - a transient miss, never a double count.
    try fs.renameAbsolute(tmp_path, parquet_path);
    try fsyncDirOf(parquet_path);

    return RollResult{ .parquet_path = parquet_path, .row_count = rows_written };
}

/// Roll every table that has data. Returns the count of tables rolled.
pub fn rollAll(allocator: Allocator, root: []const u8, stg: *staging.Staging) !usize {
    var rolled: usize = 0;
    for (Table.all) |table| {
        if (try rollTable(allocator, root, stg, table)) |res| {
            allocator.free(res.parquet_path);
            rolled += 1;
        }
    }
    return rolled;
}

/// Recover the roll temp files a crash can leave behind, per partition leaf,
/// across every table's tree. Idempotent (a no-op when there are none) and safe
/// to call at daemon startup. Only ONE suffix is handled:
///
///   - `*.parquet.tmp` (ROLL temp): a crash BETWEEN a roll's staging reset and
///     its rename leaves a durable temp whose rows are already gone from staging,
///     so the temp is the only copy. REPUBLISH it: rename -> `*.parquet`.
///
/// We collect the work into a list first (a single walk per table), then mutate,
/// so we never rename entries out from under the live walker.
///
/// NOTE: the daemon startup path wires this in at cutover. It is intentionally
/// NOT hooked into any live loop in this foundation PR - call it once, before the
/// first query/roll, after the process restarts.
pub fn recoverOrphanTemps(allocator: Allocator, root: []const u8) !void {
    for (Table.all) |table| {
        const dir_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ root, table.name() });
        defer allocator.free(dir_path);

        var dir = fs.openDirAbsolute(dir_path, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        defer dir.close();

        // Collect absolute paths in a single walk, so we never mutate the tree
        // while the walker iterates it.
        var roll_tmps = std.ArrayList([]u8){};
        defer {
            for (roll_tmps.items) |p| allocator.free(p);
            roll_tmps.deinit(allocator);
        }

        var walker = try dir.walk(allocator);
        defer walker.deinit();
        while (try walker.next()) |entry| {
            if (entry.kind != .file) continue;
            if (!std.mem.endsWith(u8, entry.basename, ".parquet.tmp")) continue;
            // entry.path is relative to dir_path; build the absolute path.
            const abs = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ dir_path, entry.path });
            errdefer allocator.free(abs);
            try roll_tmps.append(allocator, abs);
        }

        // ROLL temps: republish (rename -> `.parquet`, stripping ".tmp").
        for (roll_tmps.items) |tmp_abs| {
            const final_abs = tmp_abs[0 .. tmp_abs.len - ".tmp".len];
            try fs.renameAbsolute(tmp_abs, final_abs);
            try fsyncDirOf(final_abs);
        }
    }
}

// ============================================================================
// Retention
// ============================================================================

/// Delete whole `date=YYYY-MM-DD` partition directories older than the retention
/// window across every table. Replaces storage.runRetention's per-row DELETE:
/// the hot tier is partitioned by day, so dropping an entire stale day's
/// directory tree reclaims its space in one unlink-tree without rewriting any
/// parquet. Best-effort per partition - a failure to remove one dir is logged
/// and the rest still proceed (the cloud holds the long-term history).
///
/// A partition is stale when its day's END (date + 1 day) is before the cutoff
/// (now - retention_seconds), so a partition is only dropped once its entire day
/// is past the window. `retention_seconds <= 0` disables retention (keep all).
///
/// Deletion runs UNDER the EXCLUSIVE roll lock for the whole sweep: a query
/// freezes its parquet file list under the SHARED roll lock (see
/// initParquetQuery), so deleting a whole `date=` dir without the lock could
/// unlink a partition mid-read. Holding EX makes retention mutually exclusive
/// with that snapshot. Best-effort per partition (log + continue on failure),
/// but the lock is held across the entire sweep.
pub fn runRetention(allocator: Allocator, root: []const u8, retention_seconds: i64) !void {
    if (retention_seconds <= 0) return;
    const cutoff = std.time.timestamp() - retention_seconds;

    var lock_file = try staging.openRollLock(root, allocator);
    defer lock_file.close();
    try std.posix.flock(lock_file.handle, std.posix.LOCK.EX);
    defer std.posix.flock(lock_file.handle, std.posix.LOCK.UN) catch {};

    for (Table.all) |table| {
        const table_dir = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ root, table.name() });
        defer allocator.free(table_dir);

        var dir = fs.openDirAbsolute(table_dir, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        defer dir.close();

        var it = dir.iterate();
        while (try it.next()) |entry| {
            if (entry.kind != .directory) continue;
            if (!std.mem.startsWith(u8, entry.name, "date=")) continue;

            const day_start = dayStartUnix(entry.name["date=".len..]) orelse continue;
            // Keep the day until its whole span (start + 86400) is past the cutoff.
            if (day_start + std.time.s_per_day > cutoff) continue;

            const part_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ table_dir, entry.name });
            defer allocator.free(part_path);
            fs.deleteTreeAbsolute(part_path) catch |err| {
                std.log.warn("retention: failed to remove stale partition {s}: {}", .{ part_path, err });
                continue;
            };
        }
    }
}

/// Parse a `YYYY-MM-DD` partition name into the unix-seconds start of that day
/// (UTC midnight). Returns null on a malformed name so retention skips it rather
/// than miscomputing an age. Mirrors buildHivePath's date formatting.
fn dayStartUnix(date_str: []const u8) ?i64 {
    if (date_str.len != 10 or date_str[4] != '-' or date_str[7] != '-') return null;
    const year = std.fmt.parseInt(u16, date_str[0..4], 10) catch return null;
    const month = std.fmt.parseInt(u8, date_str[5..7], 10) catch return null;
    const day = std.fmt.parseInt(u8, date_str[8..10], 10) catch return null;
    if (month < 1 or month > 12 or day < 1 or day > 31) return null;

    // Days since the unix epoch for this calendar date (UTC), via the same
    // std.time.epoch math buildHivePath uses in reverse.
    var days: u64 = 0;
    var y: u16 = 1970;
    while (y < year) : (y += 1) {
        days += if (std.time.epoch.isLeapYear(y)) @as(u64, 366) else 365;
    }
    var m: u8 = 1;
    while (m < month) : (m += 1) {
        days += std.time.epoch.getDaysInMonth(year, @as(std.time.epoch.Month, @enumFromInt(m)));
    }
    days += day - 1;
    return @as(i64, @intCast(days)) * std.time.s_per_day;
}

// ============================================================================
// One-shot legacy metrics.db migration
// ============================================================================

/// One-shot migration of a pre-cutover resident `metrics.db` into the parquet
/// tree. If `db_path` exists, opens it READ-ONLY, COPYs each of the six tables'
/// rows into a SINGLE DETERMINISTIC file `<root>/<table>/migrated.parquet`, then
/// renames `metrics.db` -> `metrics.db.migrated` (kept for rollback, NOT
/// deleted). Best-effort: ANY failure logs a warning and returns without
/// renaming, so a partial migration can be retried (or ignored - the cloud has
/// the history). Returns true when a migration ran (db existed), false when
/// there was nothing to migrate.
///
/// IDEMPOTENCY: writing one fixed path per table (NOT PARTITION_BY, which emits
/// hash-named files) means a crash-retry after copying but before the
/// metrics.db -> .migrated rename OVERWRITES the same file rather than appending
/// a second differently-named copy - so pre-cutover history is never
/// double-counted. The migrated rows live in this single file OUTSIDE the date=
/// partitions, so retention (which deletes date= dirs) does not expire them;
/// acceptable for a bounded one-time best-effort migration.
pub fn migrateLegacyDb(allocator: Allocator, root: []const u8, db_path: []const u8) !bool {
    fs.accessAbsolute(db_path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };

    var db: c.duckdb_database = undefined;
    // Open the legacy DB read-only so we never mutate it (rollback stays clean).
    var config: c.duckdb_config = undefined;
    if (c.duckdb_create_config(&config) == c.DuckDBError) return RollError.DatabaseError;
    defer c.duckdb_destroy_config(&config);
    _ = c.duckdb_set_config(config, "access_mode", "READ_ONLY");

    const db_path_z = try allocator.dupeZ(u8, db_path);
    defer allocator.free(db_path_z);
    var open_err: [*c]u8 = null;
    if (c.duckdb_open_ext(db_path_z.ptr, &db, config, &open_err) == c.DuckDBError) {
        if (open_err != null) {
            std.log.warn("migrate: cannot open legacy db {s}: {s}", .{ db_path, open_err });
            c.duckdb_free(open_err);
        }
        return RollError.DatabaseError;
    }
    defer c.duckdb_close(&db);

    var conn: c.duckdb_connection = undefined;
    if (c.duckdb_connect(db, &conn) == c.DuckDBError) return RollError.ConnectionError;
    defer c.duckdb_disconnect(&conn);

    // COPY each table into a single deterministic <table>/migrated.parquet file
    // (see migrateLegacyDb's IDEMPOTENCY note). A table missing from a legacy DB
    // is tolerated (logged, skipped).
    for (Table.all) |table| {
        migrateTable(allocator, conn, root, table) catch |err| {
            std.log.warn("migrate: table {s} failed: {} - continuing", .{ table.name(), err });
            _ = malloc_trim(0);
            return false; // best-effort: leave metrics.db in place for retry/rollback
        };
    }
    _ = malloc_trim(0);

    // Rename metrics.db -> metrics.db.migrated (kept for rollback; do NOT delete).
    const migrated_path = try std.fmt.allocPrint(allocator, "{s}.migrated", .{db_path});
    defer allocator.free(migrated_path);
    try fs.renameAbsolute(db_path, migrated_path);
    return true;
}

fn migrateTable(allocator: Allocator, conn: c.duckdb_connection, root: []const u8, table: Table) !void {
    // A legacy DB predating a table simply lacks it; that is NOT a migration
    // failure - skip it silently. Probe the catalog first so a missing table
    // does not abort the whole migration.
    if (!try tableExists(allocator, conn, table)) {
        std.log.info("migrate: table {s} absent in legacy db, skipping", .{table.name()});
        return;
    }

    // Write ONE deterministic file <root>/<table>/migrated.parquet (no
    // PARTITION_BY). A re-run after a crash overwrites this same path, so the
    // migration is idempotent and never double-counts. The query glob
    // <root>/<table>/**/*.parquet still matches this file; with
    // hive_partitioning the absent date=/hour= keys read as NULL, which is fine -
    // queries filter on the real `timestamp` column, not the hive keys.
    const out_dir = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ root, table.name() });
    defer allocator.free(out_dir);
    try makePathAbsolute(out_dir);

    const out_path = try std.fmt.allocPrint(allocator, "{s}/migrated.parquet", .{out_dir});
    defer allocator.free(out_path);

    const escaped_path = try escapeSqlLiteral(allocator, out_path);
    defer allocator.free(escaped_path);

    const sql = try std.fmt.allocPrintSentinel(
        allocator,
        "COPY (SELECT * FROM {s}) TO '{s}' (FORMAT parquet, COMPRESSION zstd)",
        .{ table.name(), escaped_path },
        0,
    );
    defer allocator.free(sql);

    var result: c.duckdb_result = undefined;
    const state = c.duckdb_query(conn, sql.ptr, &result);
    defer c.duckdb_destroy_result(&result);
    if (state == c.DuckDBError) {
        std.log.warn("migrate COPY {s} error: {s}", .{ table.name(), c.duckdb_result_error(&result) });
        return RollError.CopyError;
    }
}

/// True when `table` exists in the connected DB's catalog. Used so the migration
/// can skip tables a legacy DB never created without treating them as failures.
fn tableExists(allocator: Allocator, conn: c.duckdb_connection, table: Table) !bool {
    const sql = try std.fmt.allocPrintSentinel(
        allocator,
        "SELECT 1 FROM information_schema.tables WHERE table_name = '{s}'",
        .{table.name()},
        0,
    );
    defer allocator.free(sql);
    var result: c.duckdb_result = undefined;
    if (c.duckdb_query(conn, sql.ptr, &result) == c.DuckDBError) {
        c.duckdb_destroy_result(&result);
        return RollError.DatabaseError;
    }
    defer c.duckdb_destroy_result(&result);
    return c.duckdb_row_count(&result) > 0;
}

// ============================================================================
// DuckDB-backed parquet write
// ============================================================================

fn writeParquet(allocator: Allocator, table: Table, snap: *Snapshot, parquet_path: []const u8) !usize {
    var db: c.duckdb_database = undefined;
    if (c.duckdb_open(":memory:", &db) == c.DuckDBError) return RollError.DatabaseError;
    defer c.duckdb_close(&db);

    var conn: c.duckdb_connection = undefined;
    if (c.duckdb_connect(db, &conn) == c.DuckDBError) return RollError.ConnectionError;
    defer c.duckdb_disconnect(&conn);

    try createTable(conn, table);
    const n = try appendSnapshot(conn, table, snap);
    try copyToParquet(allocator, conn, table, parquet_path);

    // Tear DuckDB down (defers above) then return freed pages to the OS - the
    // roll is the only point DuckDB code pages map resident.
    _ = malloc_trim(0);
    return n;
}

/// CREATE TABLE matching today's storage.zig schema for the table (same column
/// names + types) so the resulting parquet has the columns the query views
/// expect.
///
/// The u64 byte/size columns (mem_total, swap_*, *_bytes, maxmem, uptime) are
/// declared BIGINT and appended via duckdb_append_uint64, exactly as
/// storage.zig does. This signedness pairing is intentional for cutover parity
/// (do NOT switch to UBIGINT); it assumes these values stay below 2^63.
fn createTable(conn: c.duckdb_connection, table: Table) !void {
    const sql = switch (table) {
        .metrics =>
        \\CREATE TABLE metrics (
        \\  timestamp TIMESTAMP NOT NULL,
        \\  cpu_percent REAL, cpu_user REAL, cpu_system REAL, cpu_iowait REAL,
        \\  mem_total BIGINT, mem_used BIGINT, mem_percent REAL,
        \\  swap_total BIGINT, swap_used BIGINT
        \\)
        ,
        .processes =>
        \\CREATE TABLE processes (
        \\  timestamp TIMESTAMP NOT NULL, pid INTEGER, name VARCHAR, cmdline VARCHAR,
        \\  state CHAR(1), cpu_percent REAL, mem_rss BIGINT, threads INTEGER,
        \\  username VARCHAR, io_read_bytes BIGINT, io_write_bytes BIGINT,
        \\  cgroup VARCHAR, unit VARCHAR
        \\)
        ,
        .disks =>
        \\CREATE TABLE disks (
        \\  timestamp TIMESTAMP NOT NULL, mount_point VARCHAR, filesystem VARCHAR,
        \\  total_bytes BIGINT, used_bytes BIGINT, percent REAL
        \\)
        ,
        .containers =>
        \\CREATE TABLE containers (
        \\  timestamp TIMESTAMP NOT NULL, vmid INTEGER NOT NULL, name VARCHAR,
        \\  node VARCHAR, type VARCHAR, status VARCHAR, maxmem BIGINT,
        \\  maxcpu DOUBLE, uptime BIGINT
        \\)
        ,
        .logs =>
        \\CREATE TABLE logs (
        \\  timestamp TIMESTAMP NOT NULL, source VARCHAR, unit VARCHAR,
        \\  identifier VARCHAR, systemd_unit VARCHAR, priority INTEGER,
        \\  message TEXT, pid INTEGER
        \\)
        ,
        .container_metrics =>
        \\CREATE TABLE container_metrics (
        \\  timestamp TIMESTAMP NOT NULL, vmid INTEGER NOT NULL,
        \\  cpu_pct DOUBLE, mem_current BIGINT, mem_max BIGINT
        \\)
        ,
    };

    var result: c.duckdb_result = undefined;
    const state = c.duckdb_query(conn, sql.ptr, &result);
    defer c.duckdb_destroy_result(&result);
    if (state == c.DuckDBError) {
        std.log.err("roll createTable error: {s}", .{c.duckdb_result_error(&result)});
        return RollError.SchemaError;
    }
}

/// Appender-insert every decoded cycle's rows. Mirrors storage.zig's appender
/// column ordering exactly so the parquet schema lines up.
fn appendSnapshot(conn: c.duckdb_connection, table: Table, snap: *Snapshot) !usize {
    var appender: c.duckdb_appender = undefined;
    if (c.duckdb_appender_create(conn, null, table.name().ptr, &appender) == c.DuckDBError) {
        return RollError.AppendError;
    }
    defer _ = c.duckdb_appender_destroy(&appender);

    const n = try appendSnapshotTo(@ptrCast(appender), snap);

    // The per-column duckdb_append_* calls inside appendSnapshotTo do not check
    // their return codes individually; the end_row and this flush are the
    // catch-all - any append failure surfaces here as an AppendError.
    if (c.duckdb_appender_flush(appender) == c.DuckDBError) {
        std.log.err("roll appender flush error: {s}", .{c.duckdb_appender_error(appender)});
        return RollError.AppendError;
    }
    return n;
}

/// Append a snapshot's rows through an EXISTING appender (does not create/flush
/// it). Shared by the roll and the query path's staging-temp loader so the
/// column ordering lives in one place. The appender is taken as an opaque
/// pointer because the caller (parquet_query) has its own @cImport of duckdb.h
/// whose `duckdb_appender` is a distinct-but-identical type; both modules link
/// the same libduckdb so the underlying handle is interchangeable. Returns the
/// rows appended.
pub fn appendSnapshotTo(opaque_appender: ?*anyopaque, snap: *Snapshot) !usize {
    const appender: c.duckdb_appender = @ptrCast(@alignCast(opaque_appender));
    var n: usize = 0;
    for (snap.cycles) |cycle| {
        switch (cycle) {
            .metrics => |rows| for (rows) |row| {
                try appendMetricsRow(appender, row.timestamp, row.metrics);
                n += 1;
            },
            .processes => |ts| for (ts.rows) |proc| {
                try appendProcessRow(appender, ts.timestamp, proc);
                n += 1;
            },
            .disks => |ts| for (ts.rows) |disk| {
                try appendDiskRow(appender, ts.timestamp, disk);
                n += 1;
            },
            .containers => |ts| for (ts.rows) |entry| {
                try appendContainerRow(appender, ts.timestamp, entry);
                n += 1;
            },
            .logs => |rows| for (rows) |entry| {
                try appendLogRow(appender, entry);
                n += 1;
            },
            .container_metrics => |ts| for (ts.rows) |sample| {
                try appendContainerMetricRow(appender, ts.timestamp, sample);
                n += 1;
            },
        }
    }
    return n;
}

fn endRow(appender: c.duckdb_appender) !void {
    if (c.duckdb_appender_end_row(appender) == c.DuckDBError) {
        std.log.err("roll append end_row error: {s}", .{c.duckdb_appender_error(appender)});
        return RollError.AppendError;
    }
}

fn tsMicros(timestamp: i64) c.duckdb_timestamp {
    // Timestamps reach here from CRC-only-validated segment bytes, so a garbage
    // decoded value could overflow i64 on `* 1_000_000` and panic/wrap. Saturate
    // to the i64 bounds instead so the worst case is a clamped (not corrupt or
    // crashing) timestamp.
    const micros: i64 = std.math.mul(i64, timestamp, 1_000_000) catch
        if (timestamp < 0) std.math.minInt(i64) else std.math.maxInt(i64);
    return .{ .micros = micros };
}

fn appendMetricsRow(appender: c.duckdb_appender, timestamp: i64, m: collector.SystemMetrics) !void {
    _ = c.duckdb_append_timestamp(appender, tsMicros(timestamp));
    _ = c.duckdb_append_float(appender, m.cpu_percent);
    _ = c.duckdb_append_float(appender, m.cpu_user);
    _ = c.duckdb_append_float(appender, m.cpu_system);
    _ = c.duckdb_append_float(appender, m.cpu_iowait);
    _ = c.duckdb_append_uint64(appender, m.mem_total);
    _ = c.duckdb_append_uint64(appender, m.mem_used);
    _ = c.duckdb_append_float(appender, m.mem_percent);
    _ = c.duckdb_append_uint64(appender, m.swap_total);
    _ = c.duckdb_append_uint64(appender, m.swap_used);
    try endRow(appender);
}

fn appendProcessRow(appender: c.duckdb_appender, timestamp: i64, p: collector.ProcessInfo) !void {
    _ = c.duckdb_append_timestamp(appender, tsMicros(timestamp));
    _ = c.duckdb_append_uint32(appender, p.pid);
    _ = c.duckdb_append_varchar_length(appender, p.name.ptr, p.name.len);
    _ = c.duckdb_append_varchar_length(appender, p.cmdline.ptr, p.cmdline.len);
    const state_str = [_]u8{p.state};
    _ = c.duckdb_append_varchar_length(appender, &state_str, 1);
    _ = c.duckdb_append_float(appender, p.cpu_percent);
    _ = c.duckdb_append_uint64(appender, p.mem_rss);
    _ = c.duckdb_append_uint32(appender, p.threads);
    _ = c.duckdb_append_varchar_length(appender, p.username.ptr, p.username.len);
    _ = c.duckdb_append_uint64(appender, p.io_read_bytes);
    _ = c.duckdb_append_uint64(appender, p.io_write_bytes);
    _ = c.duckdb_append_varchar_length(appender, p.cgroup.ptr, p.cgroup.len);
    _ = c.duckdb_append_varchar_length(appender, p.unit.ptr, p.unit.len);
    try endRow(appender);
}

fn appendDiskRow(appender: c.duckdb_appender, timestamp: i64, d: collector.DiskInfo) !void {
    _ = c.duckdb_append_timestamp(appender, tsMicros(timestamp));
    _ = c.duckdb_append_varchar_length(appender, d.mount_point.ptr, d.mount_point.len);
    _ = c.duckdb_append_varchar_length(appender, d.filesystem.ptr, d.filesystem.len);
    _ = c.duckdb_append_uint64(appender, d.total_bytes);
    _ = c.duckdb_append_uint64(appender, d.used_bytes);
    _ = c.duckdb_append_float(appender, d.percent);
    try endRow(appender);
}

fn appendContainerRow(appender: c.duckdb_appender, timestamp: i64, e: proxmox.ContainerEntry) !void {
    _ = c.duckdb_append_timestamp(appender, tsMicros(timestamp));
    _ = c.duckdb_append_uint32(appender, e.vmid);
    _ = c.duckdb_append_varchar_length(appender, e.name.ptr, e.name.len);
    _ = c.duckdb_append_varchar_length(appender, e.node.ptr, e.node.len);
    _ = c.duckdb_append_varchar_length(appender, e.type.ptr, e.type.len);
    _ = c.duckdb_append_varchar_length(appender, e.status.ptr, e.status.len);
    _ = c.duckdb_append_uint64(appender, e.maxmem);
    _ = c.duckdb_append_double(appender, e.maxcpu);
    _ = c.duckdb_append_uint64(appender, e.uptime);
    try endRow(appender);
}

fn appendLogRow(appender: c.duckdb_appender, e: logs.LogEntry) !void {
    _ = c.duckdb_append_timestamp(appender, tsMicros(e.timestamp));
    _ = c.duckdb_append_varchar_length(appender, e.source.ptr, e.source.len);
    if (e.unit) |u| {
        _ = c.duckdb_append_varchar_length(appender, u.ptr, u.len);
    } else _ = c.duckdb_append_null(appender);
    if (e.identifier) |id| {
        _ = c.duckdb_append_varchar_length(appender, id.ptr, id.len);
    } else _ = c.duckdb_append_null(appender);
    if (e.systemd_unit) |su| {
        _ = c.duckdb_append_varchar_length(appender, su.ptr, su.len);
    } else _ = c.duckdb_append_null(appender);
    _ = c.duckdb_append_int32(appender, e.priority);
    _ = c.duckdb_append_varchar_length(appender, e.message.ptr, e.message.len);
    if (e.pid) |pid| {
        _ = c.duckdb_append_uint32(appender, pid);
    } else _ = c.duckdb_append_null(appender);
    try endRow(appender);
}

/// Mirrors storage.insertContainerMetrics exactly: a NaN cpu_pct (the
/// "first cycle, no delta yet" sentinel) and a null mem_max are persisted as
/// SQL NULL so the dashboard doesn't render a misleading 0% spike / 0 limit.
fn appendContainerMetricRow(appender: c.duckdb_appender, timestamp: i64, m: proxmox.ContainerMetrics) !void {
    _ = c.duckdb_append_timestamp(appender, tsMicros(timestamp));
    _ = c.duckdb_append_uint32(appender, m.vmid);
    if (std.math.isNan(m.cpu_pct)) {
        _ = c.duckdb_append_null(appender);
    } else {
        _ = c.duckdb_append_double(appender, m.cpu_pct);
    }
    _ = c.duckdb_append_uint64(appender, m.mem_current);
    if (m.mem_max) |mm| {
        _ = c.duckdb_append_uint64(appender, mm);
    } else {
        _ = c.duckdb_append_null(appender);
    }
    try endRow(appender);
}

fn copyToParquet(allocator: Allocator, conn: c.duckdb_connection, table: Table, parquet_path: []const u8) !void {
    // Single-quotes in a filesystem path would break the SQL literal; our hive
    // paths never contain them, but escape defensively all the same.
    const escaped = try escapeSqlLiteral(allocator, parquet_path);
    defer allocator.free(escaped);

    const sql = try std.fmt.allocPrintSentinel(
        allocator,
        "COPY (SELECT * FROM {s}) TO '{s}' (FORMAT parquet, COMPRESSION zstd)",
        .{ table.name(), escaped },
        0,
    );
    defer allocator.free(sql);

    var result: c.duckdb_result = undefined;
    const state = c.duckdb_query(conn, sql.ptr, &result);
    defer c.duckdb_destroy_result(&result);
    if (state == c.DuckDBError) {
        std.log.err("roll COPY TO parquet error: {s}", .{c.duckdb_result_error(&result)});
        return RollError.CopyError;
    }
}

// ============================================================================
// Path + hashing + fsync helpers
// ============================================================================

/// Hex of a stable hash over the segment's raw bytes. Same segment -> same id.
fn seqFromBytes(allocator: Allocator, data: []const u8) ![]u8 {
    const h = std.hash.Wyhash.hash(0, data);
    return std.fmt.allocPrint(allocator, "{x:0>16}", .{h});
}

/// First cycle's timestamp, used to choose the date=/hour= partition.
fn firstTimestamp(snap: Snapshot) ?i64 {
    if (snap.cycles.len == 0) return null;
    return switch (snap.cycles[0]) {
        .metrics => |rows| if (rows.len > 0) rows[0].timestamp else null,
        .processes => |ts| ts.timestamp,
        .disks => |ts| ts.timestamp,
        .containers => |ts| ts.timestamp,
        .logs => |rows| if (rows.len > 0) rows[0].timestamp else null,
        .container_metrics => |ts| ts.timestamp,
    };
}

/// <root>/<table>/date=YYYY-MM-DD/hour=HH/<seq>.parquet, creating the
/// directories. Caller owns the returned path.
pub fn buildHivePath(allocator: Allocator, root: []const u8, table: Table, timestamp: i64, seq: []const u8) ![]u8 {
    // `timestamp` is decoded from CRC-only-validated bytes, so it can be garbage.
    // A negative value would panic on @intCast to the u64 EpochSeconds.secs, and a
    // huge POSITIVE value would overflow std.time.epoch's year/day math and panic.
    // Clamp to [0, 253402300799] (1970-01-01 .. 9999-12-31 in unix seconds) so a
    // garbage timestamp lands in a sane fallback partition instead of crashing.
    const max_unix_secs: i64 = 253402300799;
    const safe_ts: u64 = @intCast(std.math.clamp(timestamp, 0, max_unix_secs));
    const ep = std.time.epoch.EpochSeconds{ .secs = safe_ts };
    const day = ep.getEpochDay();
    const year_day = day.calculateYearDay();
    const md = year_day.calculateMonthDay();
    const ds = ep.getDaySeconds();

    const dir = try std.fmt.allocPrint(allocator, "{s}/{s}/date={d:0>4}-{d:0>2}-{d:0>2}/hour={d:0>2}", .{
        root,
        table.name(),
        year_day.year,
        md.month.numeric(),
        md.day_index + 1,
        ds.getHoursIntoDay(),
    });
    defer allocator.free(dir);

    try makePathAbsolute(dir);

    return std.fmt.allocPrint(allocator, "{s}/{s}.parquet", .{ dir, seq });
}

// The <table>/date=/hour= directories are chmod'd to 0700 (not the default
// 0755) so a local unprivileged user cannot enumerate table names or infer
// collection timing from directory mtimes - consistent with the 0600 segment +
// parquet files and storage.zig's 0600 DB.
const hive_dir_mode: fs.File.Mode = 0o700;

/// Recursively create an absolute directory path (mkdir -p). Newly-created
/// components are chmod'd to 0700; pre-existing components are left alone.
fn makePathAbsolute(path: []const u8) !void {
    // std.fs.makeDirAbsolute is single-level; walk the components.
    var i: usize = 0;
    while (std.mem.indexOfScalarPos(u8, path, i + 1, '/')) |idx| {
        const prefix = path[0..idx];
        if (prefix.len == 0) {
            i = idx;
            continue;
        }
        try makeDirAbsolute0700(prefix);
        i = idx;
    }
    try makeDirAbsolute0700(path);
}

/// makeDirAbsolute that chmods a freshly-created dir to 0700 best-effort. An
/// already-existing dir is left untouched (its owner already chose its mode).
fn makeDirAbsolute0700(path: []const u8) !void {
    fs.makeDirAbsolute(path) catch |err| switch (err) {
        error.PathAlreadyExists => return,
        else => return err,
    };
    // Path-based chmod via the raw syscall (same direct-syscall idiom as the
    // directory fsync below): an O_RDONLY dir fd cannot be fchmod'd on Linux.
    var buf: [fs.max_path_bytes]u8 = undefined;
    if (path.len >= buf.len) return;
    @memcpy(buf[0..path.len], path);
    buf[path.len] = 0;
    const rc = std.os.linux.chmod(@ptrCast(&buf), hive_dir_mode);
    if (std.os.linux.E.init(rc) != .SUCCESS) {
        std.log.warn("hive dir chmod 0700 failed for {s}: errno {}", .{ path, std.os.linux.E.init(rc) });
    }
}

/// fsync a written file and its parent directory so the file and its directory
/// entry both survive a crash. The FILE fsync is load-bearing for durability and
/// its failure is propagated (NOT swallowed) - the roll must not proceed to reset
/// staging on un-durable data. The directory fsync is tolerant of EINVAL (some
/// filesystems reject directory fsync) but propagates other errnos.
fn fsyncFileAndDir(path: []const u8) !void {
    {
        const f = try fs.openFileAbsolute(path, .{});
        defer f.close();
        try f.sync();
    }
    const dir_path = std.fs.path.dirname(path) orelse return;
    try fsyncDir(dir_path);
}

/// fsync just the parent directory of `path` (so a rename's new directory entry
/// is durable). Same EINVAL-tolerant policy as fsyncFileAndDir's dir fsync.
fn fsyncDirOf(path: []const u8) !void {
    const dir_path = std.fs.path.dirname(path) orelse return;
    try fsyncDir(dir_path);
}

/// Open a directory read-only (O_RDONLY) and fsync it so a just-created or just-
/// renamed entry is durable. EINVAL is tolerated (some filesystems reject
/// directory fsync); any other errno is propagated rather than ignored.
fn fsyncDir(dir_path: []const u8) !void {
    var dir = try fs.openDirAbsolute(dir_path, .{ .iterate = true });
    defer dir.close();
    const rc = std.os.linux.fsync(dir.fd);
    switch (std.os.linux.E.init(rc)) {
        .SUCCESS, .INVAL => {},
        else => |e| {
            std.log.err("directory fsync failed for {s}: errno {}", .{ dir_path, e });
            return error.SyncError;
        },
    }
}

/// Double every single quote so `s` is safe to interpolate inside a single-
/// quoted SQL string literal. Shared by the roll's COPY and the query path's
/// read_parquet file-list so a path containing a quote can neither break the
/// SQL nor inject. Caller owns the result.
pub fn escapeSqlLiteral(allocator: Allocator, s: []const u8) ![]u8 {
    var out = std.ArrayList(u8){};
    errdefer out.deinit(allocator);
    for (s) |ch| {
        if (ch == '\'') try out.append(allocator, '\'');
        try out.append(allocator, ch);
    }
    return out.toOwnedSlice(allocator);
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

fn testRoot(allocator: Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    return tmp.dir.realpathAlloc(allocator, ".");
}

fn sampleMetrics() collector.SystemMetrics {
    return .{
        .cpu_percent = 25.5,
        .cpu_user = 18.0,
        .cpu_system = 7.0,
        .cpu_iowait = 0.5,
        .mem_total = 32_000_000_000,
        .mem_used = 16_000_000_000,
        .mem_percent = 50.0,
        .swap_total = 0,
        .swap_used = 0,
    };
}

/// Read a scalar count back from a parquet file via a fresh transient DuckDB -
/// the same read_parquet path queries use.
fn parquetCount(allocator: Allocator, parquet_path: []const u8) !i64 {
    var db: c.duckdb_database = undefined;
    try testing.expect(c.duckdb_open(":memory:", &db) != c.DuckDBError);
    defer c.duckdb_close(&db);
    var conn: c.duckdb_connection = undefined;
    try testing.expect(c.duckdb_connect(db, &conn) != c.DuckDBError);
    defer c.duckdb_disconnect(&conn);

    const sql = try std.fmt.allocPrintSentinel(allocator, "SELECT COUNT(*) FROM read_parquet('{s}')", .{parquet_path}, 0);
    defer allocator.free(sql);

    var result: c.duckdb_result = undefined;
    try testing.expect(c.duckdb_query(conn, sql.ptr, &result) != c.DuckDBError);
    defer c.duckdb_destroy_result(&result);
    return c.duckdb_value_int64(&result, 0, 0);
}

fn parquetFloat(allocator: Allocator, parquet_path: []const u8, col: []const u8) !f64 {
    var db: c.duckdb_database = undefined;
    try testing.expect(c.duckdb_open(":memory:", &db) != c.DuckDBError);
    defer c.duckdb_close(&db);
    var conn: c.duckdb_connection = undefined;
    try testing.expect(c.duckdb_connect(db, &conn) != c.DuckDBError);
    defer c.duckdb_disconnect(&conn);

    const sql = try std.fmt.allocPrintSentinel(allocator, "SELECT {s} FROM read_parquet('{s}') LIMIT 1", .{ col, parquet_path }, 0);
    defer allocator.free(sql);

    var result: c.duckdb_result = undefined;
    try testing.expect(c.duckdb_query(conn, sql.ptr, &result) != c.DuckDBError);
    defer c.duckdb_destroy_result(&result);
    return c.duckdb_value_double(&result, 0, 0);
}

test "roll: metrics segment to parquet, re-reads with correct count and values" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    var stg = try staging.Staging.open(a, root);
    defer stg.deinit();

    const n = 7;
    const m = sampleMetrics();
    var i: i64 = 0;
    while (i < n) : (i += 1) {
        try stg.appendMetrics(1_700_000_000 + i * 10, m);
    }
    try stg.sync();

    const res = (try rollTable(a, root, &stg, .metrics)) orelse return error.TestUnexpectedResult;
    defer a.free(res.parquet_path);

    try testing.expectEqual(@as(usize, n), res.row_count);

    // Parquet exists and re-reads through the same read_parquet path.
    const count = try parquetCount(a, res.parquet_path);
    try testing.expectEqual(@as(i64, n), count);

    // Spot-check a column value round-tripped.
    const cpu = try parquetFloat(a, res.parquet_path, "cpu_percent");
    try testing.expectApproxEqAbs(@as(f64, 25.5), cpu, 0.01);

    // Segment was reset (only the schema_version byte remains).
    try testing.expectEqual(@as(u64, 1), try stg.byteLen(.metrics));
}

test "roll: empty segment is a no-op" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    var stg = try staging.Staging.open(a, root);
    defer stg.deinit();

    const res = try rollTable(a, root, &stg, .metrics);
    try testing.expect(res == null);
}

test "roll: filename is idempotent for identical segment contents" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    const m = sampleMetrics();

    // Roll once.
    var path1: []u8 = undefined;
    {
        var stg = try staging.Staging.open(a, root);
        defer stg.deinit();
        try stg.appendMetrics(1_700_000_000, m);
        try stg.sync();
        const res = (try rollTable(a, root, &stg, .metrics)).?;
        path1 = res.parquet_path;
    }
    defer a.free(path1);

    // Build an identical segment again and confirm the derived path matches
    // (idempotent overwrite on a crash-replay).
    var path2: []u8 = undefined;
    {
        var stg = try staging.Staging.open(a, root);
        defer stg.deinit();
        try stg.appendMetrics(1_700_000_000, m);
        try stg.sync();
        const res = (try rollTable(a, root, &stg, .metrics)).?;
        path2 = res.parquet_path;
    }
    defer a.free(path2);

    try testing.expectEqualStrings(path1, path2);
}

test "roll: re-roll of identical (un-reset) segment is idempotent, no double-count" {
    // Simulates a crash AFTER COPY but BEFORE reset: the segment still holds the
    // same rows, so the next roll must re-derive the SAME parquet path (hash of
    // the raw bytes), overwrite it, and leave the query count correct - never
    // doubled. The concurrent interleaving of a query observing the
    // published-but-not-yet-reset window is prevented by the EX/SH roll lock in
    // rollTable / initParquetQuery; this sequential test covers the union math
    // that the lock protects.
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    const m = sampleMetrics();

    // First roll: produces parquet, then resets the segment.
    var path1: []u8 = undefined;
    {
        var stg = try staging.Staging.open(a, root);
        defer stg.deinit();
        try stg.appendMetrics(1_700_000_000, m);
        try stg.appendMetrics(1_700_000_010, m);
        try stg.sync();
        const res = (try rollTable(a, root, &stg, .metrics)).?;
        defer a.free(res.parquet_path);
        path1 = try a.dupe(u8, res.parquet_path);
        try testing.expectEqual(@as(usize, 2), res.row_count);
    }
    defer a.free(path1);

    // Rebuild the IDENTICAL segment (as a crash-replay would leave it un-reset)
    // and roll again. The path must match and the count must be unchanged.
    var path2: []u8 = undefined;
    {
        var stg = try staging.Staging.open(a, root);
        defer stg.deinit();
        try stg.appendMetrics(1_700_000_000, m);
        try stg.appendMetrics(1_700_000_010, m);
        try stg.sync();
        const res = (try rollTable(a, root, &stg, .metrics)).?;
        defer a.free(res.parquet_path);
        path2 = try a.dupe(u8, res.parquet_path);
        try testing.expectEqual(@as(usize, 2), res.row_count);
    }
    defer a.free(path2);

    try testing.expectEqualStrings(path1, path2);

    // The overwritten parquet holds exactly the 2 rows - not 4 (no duplication).
    try testing.expectEqual(@as(i64, 2), try parquetCount(a, path1));
}

test "roll: out-of-range / negative timestamp rolls without panicking" {
    // A garbage decoded timestamp (negative, and a huge positive that would
    // overflow tsMicros' * 1_000_000 AND buildHivePath's epoch/day math) must not
    // panic; it should land in a fallback partition and still produce a readable
    // parquet. firstTimestamp uses row 0 to pick the partition, so we exercise
    // BOTH a huge-positive and a negative value AS THE FIRST ROW (one segment
    // each) to prove buildHivePath's clamp handles either extreme.
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    const m = sampleMetrics();

    // Segment 1: huge-positive FIRST row (the partition timestamp).
    {
        var stg = try staging.Staging.open(a, root);
        defer stg.deinit();
        try stg.appendMetrics(std.math.maxInt(i64), m); // would overflow * 1e6 + epoch math
        try stg.appendMetrics(-5, m); // negative, second
        try stg.sync();

        const res = (try rollTable(a, root, &stg, .metrics)) orelse return error.TestUnexpectedResult;
        defer a.free(res.parquet_path);
        try testing.expectEqual(@as(usize, 2), res.row_count);
        try testing.expectEqual(@as(i64, 2), try parquetCount(a, res.parquet_path));
    }

    // Segment 2: negative FIRST row (the partition timestamp).
    {
        var stg = try staging.Staging.open(a, root);
        defer stg.deinit();
        try stg.appendMetrics(-5, m); // negative first
        try stg.appendMetrics(std.math.maxInt(i64), m);
        try stg.sync();

        const res = (try rollTable(a, root, &stg, .metrics)) orelse return error.TestUnexpectedResult;
        defer a.free(res.parquet_path);
        try testing.expectEqual(@as(usize, 2), res.row_count);
        try testing.expectEqual(@as(i64, 2), try parquetCount(a, res.parquet_path));
    }
}

test "roll: processes and logs segments to parquet" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    var stg = try staging.Staging.open(a, root);
    defer stg.deinit();

    const procs = [_]collector.ProcessInfo{.{
        .pid = 42,
        .name = "proc",
        .cmdline = "proc --run",
        .state = 'R',
        .cpu_percent = 1.0,
        .mem_rss = 1024,
        .threads = 1,
        .username = "root",
        .io_read_bytes = 0,
        .io_write_bytes = 0,
        .cgroup = "",
        .unit = "",
    }};
    try stg.appendProcesses(1_700_000_000, &procs);

    const entries = [_]logs.LogEntry{.{
        .timestamp = 1_700_000_000,
        .source = "systemd",
        .unit = "x",
        .identifier = null,
        .systemd_unit = "x.service",
        .priority = 4,
        .message = "warn",
        .pid = 9,
    }};
    try stg.appendLogs(&entries);
    try stg.sync();

    const pres = (try rollTable(a, root, &stg, .processes)).?;
    defer a.free(pres.parquet_path);
    try testing.expectEqual(@as(i64, 1), try parquetCount(a, pres.parquet_path));

    const lres = (try rollTable(a, root, &stg, .logs)).?;
    defer a.free(lres.parquet_path);
    try testing.expectEqual(@as(i64, 1), try parquetCount(a, lres.parquet_path));
}

test "roll: container_metrics segment to parquet, NaN/null persist as SQL NULL" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    var stg = try staging.Staging.open(a, root);
    defer stg.deinit();

    const samples = [_]proxmox.ContainerMetrics{
        .{ .vmid = 101, .cpu_pct = 33.0, .mem_current = 1_000_000, .mem_max = 4_000_000 },
        .{ .vmid = 102, .cpu_pct = std.math.nan(f64), .mem_current = 2_000_000, .mem_max = null },
    };
    try stg.appendContainerMetrics(1_700_000_000, &samples);
    try stg.sync();

    const res = (try rollTable(a, root, &stg, .container_metrics)).?;
    defer a.free(res.parquet_path);
    try testing.expectEqual(@as(usize, 2), res.row_count);
    try testing.expectEqual(@as(i64, 2), try parquetCount(a, res.parquet_path));

    // The NaN cpu_pct + null mem_max row must read back as SQL NULL (exactly one
    // NULL each), proving the storage.insertContainerMetrics parity.
    var db: c.duckdb_database = undefined;
    try testing.expect(c.duckdb_open(":memory:", &db) != c.DuckDBError);
    defer c.duckdb_close(&db);
    var conn: c.duckdb_connection = undefined;
    try testing.expect(c.duckdb_connect(db, &conn) != c.DuckDBError);
    defer c.duckdb_disconnect(&conn);
    const sql = try std.fmt.allocPrintSentinel(
        a,
        "SELECT COUNT(*) FROM read_parquet('{s}') WHERE cpu_pct IS NULL OR mem_max IS NULL",
        .{res.parquet_path},
        0,
    );
    defer a.free(sql);
    var result: c.duckdb_result = undefined;
    try testing.expect(c.duckdb_query(conn, sql.ptr, &result) != c.DuckDBError);
    defer c.duckdb_destroy_result(&result);
    try testing.expectEqual(@as(i64, 1), c.duckdb_value_int64(&result, 0, 0));
}

test "roll: recoverOrphanTemps renames an orphan temp to its final, queryable" {
    // Simulates a crash BETWEEN staging reset and the rename: staging is already
    // cleared and only a durable `<seq>.parquet.tmp` orphan remains. Startup
    // recovery must rename it to `<seq>.parquet` and the rows must be queryable.
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    // Produce a real parquet via a normal roll, then move it back to a `.tmp`
    // path to stand in for the orphan a mid-roll crash would leave.
    var orphan_tmp: []u8 = undefined;
    var final_path: []u8 = undefined;
    {
        var stg = try staging.Staging.open(a, root);
        defer stg.deinit();
        try stg.appendMetrics(1_700_000_000, sampleMetrics());
        try stg.appendMetrics(1_700_000_010, sampleMetrics());
        try stg.sync();
        const res = (try rollTable(a, root, &stg, .metrics)).?;
        final_path = res.parquet_path; // owned
        orphan_tmp = try std.fmt.allocPrint(a, "{s}.tmp", .{final_path});
        try fs.renameAbsolute(final_path, orphan_tmp);
    }
    defer a.free(final_path);
    defer a.free(orphan_tmp);

    // The final no longer exists; only the orphan temp does.
    try testing.expectError(error.FileNotFound, fs.accessAbsolute(final_path, .{}));

    // Recovery renames the orphan to its final name.
    try recoverOrphanTemps(a, root);

    // Final now exists and is readable; the orphan temp is gone.
    try testing.expectEqual(@as(i64, 2), try parquetCount(a, final_path));
    try testing.expectError(error.FileNotFound, fs.accessAbsolute(orphan_tmp, .{}));

    // Idempotent: a second call with no temps left is a clean no-op.
    try recoverOrphanTemps(a, root);
    try testing.expectEqual(@as(i64, 2), try parquetCount(a, final_path));
}

test "roll: runRetention drops a stale date= partition, keeps a fresh one" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    var stg = try staging.Staging.open(a, root);
    defer stg.deinit();

    // One segment far in the past (stale) and one at "now" (fresh). The roll
    // partitions by the row's own timestamp, so each lands in its own date= dir.
    const now = std.time.timestamp();
    const stale_ts = now - (30 * std.time.s_per_day); // 30 days ago
    try stg.appendMetrics(stale_ts, sampleMetrics());
    try stg.sync();
    const r1 = (try rollTable(a, root, &stg, .metrics)).?;
    a.free(r1.parquet_path);

    try stg.appendMetrics(now, sampleMetrics());
    try stg.sync();
    const r2 = (try rollTable(a, root, &stg, .metrics)).?;
    defer a.free(r2.parquet_path);

    // Two date= partitions exist before retention.
    try testing.expectEqual(@as(usize, 2), try countDatePartitions(a, root, .metrics));

    // 7-day window: the 30-day-old partition is stale; the fresh one stays.
    try runRetention(a, root, 7 * std.time.s_per_day);

    try testing.expectEqual(@as(usize, 1), try countDatePartitions(a, root, .metrics));
    // The fresh parquet is still readable.
    try testing.expectEqual(@as(i64, 1), try parquetCount(a, r2.parquet_path));
}

fn countDatePartitions(allocator: Allocator, root: []const u8, table: Table) !usize {
    const table_dir = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ root, table.name() });
    defer allocator.free(table_dir);
    var dir = fs.openDirAbsolute(table_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return 0,
        else => return err,
    };
    defer dir.close();
    var it = dir.iterate();
    var n: usize = 0;
    while (try it.next()) |e| {
        if (e.kind == .directory and std.mem.startsWith(u8, e.name, "date=")) n += 1;
    }
    return n;
}

test "roll: migrateLegacyDb copies a seeded duckdb into the parquet tree" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    const db_path = try std.fmt.allocPrint(a, "{s}/metrics.db", .{root});
    defer a.free(db_path);

    // Seed a small legacy DB with a metrics table (the schema the roll's
    // createTable produces) and two rows.
    {
        var db: c.duckdb_database = undefined;
        const db_path_z = try a.dupeZ(u8, db_path);
        defer a.free(db_path_z);
        try testing.expect(c.duckdb_open(db_path_z.ptr, &db) != c.DuckDBError);
        defer c.duckdb_close(&db);
        var conn: c.duckdb_connection = undefined;
        try testing.expect(c.duckdb_connect(db, &conn) != c.DuckDBError);
        defer c.duckdb_disconnect(&conn);

        const ddl =
            \\CREATE TABLE metrics (timestamp TIMESTAMP NOT NULL, cpu_percent REAL, cpu_user REAL,
            \\  cpu_system REAL, cpu_iowait REAL, mem_total BIGINT, mem_used BIGINT, mem_percent REAL,
            \\  swap_total BIGINT, swap_used BIGINT);
            \\INSERT INTO metrics VALUES (to_timestamp(1700000000), 1,1,1,1, 100,50,50, 0,0);
            \\INSERT INTO metrics VALUES (to_timestamp(1700000010), 2,2,2,2, 100,60,60, 0,0);
        ;
        var r: c.duckdb_result = undefined;
        try testing.expect(c.duckdb_query(conn, ddl, &r) != c.DuckDBError);
        c.duckdb_destroy_result(&r);
    }

    // Migrate.
    const ran = try migrateLegacyDb(a, root, db_path);
    try testing.expect(ran);

    // The two rows are now readable from the parquet tree (the migration writes
    // one deterministic <table>/migrated.parquet, matched by the **/*.parquet glob).
    var db: c.duckdb_database = undefined;
    try testing.expect(c.duckdb_open(":memory:", &db) != c.DuckDBError);
    defer c.duckdb_close(&db);
    var conn: c.duckdb_connection = undefined;
    try testing.expect(c.duckdb_connect(db, &conn) != c.DuckDBError);
    defer c.duckdb_disconnect(&conn);
    const sql = try std.fmt.allocPrintSentinel(a, "SELECT COUNT(*) FROM read_parquet('{s}/metrics/**/*.parquet')", .{root}, 0);
    defer a.free(sql);
    var result: c.duckdb_result = undefined;
    try testing.expect(c.duckdb_query(conn, sql.ptr, &result) != c.DuckDBError);
    defer c.duckdb_destroy_result(&result);
    try testing.expectEqual(@as(i64, 2), c.duckdb_value_int64(&result, 0, 0));

    // metrics.db was renamed to metrics.db.migrated (kept for rollback).
    try testing.expectError(error.FileNotFound, fs.accessAbsolute(db_path, .{}));
    const migrated = try std.fmt.allocPrint(a, "{s}.migrated", .{db_path});
    defer a.free(migrated);
    try fs.accessAbsolute(migrated, .{});

    // IDEMPOTENCY: simulate a crash-retry where the COPY ran but the
    // metrics.db -> .migrated rename did not. Put the legacy db back and migrate
    // AGAIN; the deterministic single file is overwritten (not appended), so the
    // row count stays 2 - never doubled to 4.
    try fs.renameAbsolute(migrated, db_path);
    try testing.expect(try migrateLegacyDb(a, root, db_path));

    var result2: c.duckdb_result = undefined;
    try testing.expect(c.duckdb_query(conn, sql.ptr, &result2) != c.DuckDBError);
    defer c.duckdb_destroy_result(&result2);
    try testing.expectEqual(@as(i64, 2), c.duckdb_value_int64(&result2, 0, 0));

    // No legacy db => no-op, returns false.
    try testing.expect(!(try migrateLegacyDb(a, root, db_path)));
}

test "roll: recoverOrphanTemps is a no-op when there are no temps" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    // No table dirs exist at all - must not error.
    try recoverOrphanTemps(a, root);

    // A normal roll leaves only a committed `.parquet`; recovery must not touch it.
    var stg = try staging.Staging.open(a, root);
    defer stg.deinit();
    try stg.appendMetrics(1_700_000_000, sampleMetrics());
    try stg.sync();
    const res = (try rollTable(a, root, &stg, .metrics)).?;
    defer a.free(res.parquet_path);

    try recoverOrphanTemps(a, root);
    try testing.expectEqual(@as(i64, 1), try parquetCount(a, res.parquet_path));
}
