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
    CompactionOutputMissing,
    CompactionCommitIncomplete,
    EmptyManifest,
    UnsafeManifestPath,
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
// Day-level compaction (plan 25 follow-up)
// ============================================================================
//
// GRAIN: one file per table per SEALED day. With a ~1h roll interval each
// `hour=HH` leaf already holds ~1 parquet, so the file count a query pays for is
// the NUMBER of hour-partitions (~24/day, ~168 over the 7-day window). Compaction
// merges a whole sealed `date=YYYY-MM-DD` directory (one strictly OLDER than
// today/UTC, so we never fight an active roll into today's hour leaf) into a
// SINGLE file `<root>/<table>/date=YYYY-MM-DD/<seq>.parquet` - note the merged
// file sits at the DAY level, NOT under an `hour=` subdir - and then removes the
// day's `hour=*` subdirs.
//
// CRASH PROTOCOL (per-compaction MANIFEST): the prior, removed compaction
// attempt double-counted twice because its recovery deleted "all files except
// the new one", which raced concurrent rolls and stacked crashes. The manifest
// fixes that by naming EXACTLY the inputs to delete, so recovery can never touch
// a file the compaction did not consume. For a sealed day dir D of table T,
// under LOCK_EX:
//   1. Enumerate every `.parquet` under D (recursively) -> input set I. |I|<=1: skip.
//   2. seq = stable hash of the SORTED input relative paths (deterministic ->
//      an interrupted compaction re-runs to the SAME output name, idempotent).
//   3. COPY the merge to `D/<seq>.parquet.building`; fsync the file AND D.
//   4. Write `D/<seq>.manifest.tmp` (output basename + each input rel path),
//      fsync it, atomic-rename -> `D/<seq>.manifest` = COMMIT POINT, fsync D.
//   5. Delete every input in I; rmdir the now-empty `hour=*` subdirs.
//   6. Rename `D/<seq>.parquet.building` -> `D/<seq>.parquet`; fsync D.
//   7. Delete `D/<seq>.manifest`.
// `.manifest`/`.building` end in neither `.parquet` nor `.parquet.tmp`, so the
// query glob (collectParquetFiles) and recoverOrphanTemps both ignore them.

const building_suffix = ".parquet.building";
const manifest_suffix = ".manifest";
const manifest_tmp_suffix = ".manifest.tmp";

/// Compact every SEALED day (a `date=` dir strictly older than today/UTC) of
/// every table that still has more than one parquet file or any `hour=` subdir.
/// Runs each day's merge under LOCK_EX (the same discipline as the roll and
/// retention) so it is mutually exclusive with rolls and query snapshots. Few
/// such dirs exist per hourly tick. Best-effort per day: a failure on one is
/// logged and the rest still proceed.
pub fn compactSealedDays(allocator: Allocator, root: []const u8) !void {
    const today = todayUtcDays();
    for (Table.all) |table| {
        const table_dir = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ root, table.name() });
        defer allocator.free(table_dir);

        var dir = fs.openDirAbsolute(table_dir, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };

        // Collect candidate sealed-day directory NAMES first (a single iterate),
        // then close the iterator before mutating the tree under each compaction.
        var days = std.ArrayList([]u8){};
        defer {
            for (days.items) |d| allocator.free(d);
            days.deinit(allocator);
        }
        {
            defer dir.close();
            var it = dir.iterate();
            while (try it.next()) |entry| {
                if (entry.kind != .directory) continue;
                if (!std.mem.startsWith(u8, entry.name, "date=")) continue;
                const day = dayIndexFromName(entry.name["date=".len..]) orelse continue;
                // SEALED guard: only days strictly OLDER than today (UTC). Today's
                // dir still receives active rolls, so leave it untouched.
                if (day >= today) continue;
                try days.append(allocator, try allocator.dupe(u8, entry.name["date=".len..]));
            }
        }

        for (days.items) |date_str| {
            compactDay(allocator, root, table, date_str) catch |err| {
                _ = malloc_trim(0);
                // A POST-COMMIT failure (CommitIncomplete) is FATAL: the manifest
                // is committed and rows may live only in the un-published building,
                // which queries ignore. Propagate so the daemon stops serving short
                // counts; a restart's recoverCompactions finishes from the durable
                // manifest. A PRE-COMMIT failure touched no inputs (the day is
                // exactly as before) - log it and move to the next day.
                if (err == error.CompactionCommitIncomplete) return err;
                std.log.warn("compaction: day {s}/{s} failed pre-commit: {} - continuing", .{ table.name(), date_str, err });
            };
        }
    }
}

/// Compact one sealed day directory `<root>/<table>/date=<date_str>` into a
/// single day-level `<seq>.parquet`, following the 7-step manifest protocol in
/// the section header. Takes LOCK_EX for the whole critical section. A no-op
/// (returns without writing) when the day has <= 1 input parquet.
pub fn compactDay(allocator: Allocator, root: []const u8, table: Table, date_str: []const u8) !void {
    var lock_file = try staging.openRollLock(root, allocator);
    defer lock_file.close();
    try std.posix.flock(lock_file.handle, std.posix.LOCK.EX);
    defer std.posix.flock(lock_file.handle, std.posix.LOCK.UN) catch {};

    const day_dir = try std.fmt.allocPrint(allocator, "{s}/{s}/date={s}", .{ root, table.name(), date_str });
    defer allocator.free(day_dir);

    // (0) Refuse to start if the day still has an UNFINISHED compaction (a
    // `.manifest` or `.building` left by a crashed run). Starting a fresh merge
    // over a now-PARTIAL input set would write a SECOND manifest/building; at the
    // next startup, recovery would publish BOTH the old building (the full input
    // set) AND the new one (the partial set), double-counting the overlap. The
    // unfinished compaction must be resolved by recoverCompactions (at the next
    // daemon start) before this day is eligible again. Skip it for now.
    if (try dayHasPendingCompaction(day_dir)) {
        std.log.warn("compaction: day {s}/{s} has a pending compaction artifact - deferring to startup recovery", .{ table.name(), date_str });
        return;
    }

    // (1) Enumerate every committed `.parquet` under the day dir, recursively,
    // as paths RELATIVE to the day dir (the manifest records relative paths so a
    // moved root still recovers, and the seq hash is root-independent).
    var inputs = try collectDayInputs(allocator, day_dir);
    defer {
        for (inputs.items) |p| allocator.free(p);
        inputs.deinit(allocator);
    }
    if (inputs.items.len <= 1) return; // nothing to merge

    // (2) Stable seq from the SORTED input relative paths -> deterministic output
    // name, so an interrupted+retried compaction targets the SAME files.
    std.mem.sort([]u8, inputs.items, {}, lessThanSlice);
    const seq = try seqFromRelPaths(allocator, inputs.items);
    defer allocator.free(seq);

    const building_path = try std.fmt.allocPrint(allocator, "{s}/{s}{s}", .{ day_dir, seq, building_suffix });
    defer allocator.free(building_path);
    const final_path = try std.fmt.allocPrint(allocator, "{s}/{s}.parquet", .{ day_dir, seq });
    defer allocator.free(final_path);

    // (3) Merge all inputs into the `.building` file, then fsync file + dir.
    try copyMergeToParquet(allocator, day_dir, inputs.items, building_path);
    try fsyncFileAndDir(building_path);

    // (4) Write + fsync the manifest tmp, then atomic-rename to the committed
    // manifest = COMMIT POINT. After this, recovery completes the deletion +
    // publish even across a crash; before it, recovery just drops the building.
    try writeManifest(allocator, day_dir, seq, final_path, inputs.items);

    // (5)-(7) Delete inputs, publish the building, drop the manifest. This is the
    // SAME completion logic startup recovery replays from the committed manifest
    // (completeCompaction), so the live path and the crash path are identical.
    //
    // A failure HERE is POST-COMMIT: the manifest is committed and rows may live
    // only in the (queries-ignore-it) building until the merge is published. We
    // surface it as a DISTINCT `CompactionCommitIncomplete` so compactSealedDays
    // treats it as FATAL (vs a best-effort pre-commit failure, which left every
    // input untouched). The committed manifest pins the exact recovery step, so a
    // restart's recoverCompactions finishes it - never leaving a partial state a
    // later compaction could double-count.
    const out_basename = std.fs.path.basename(final_path);
    completeCompaction(allocator, day_dir, seq, inputs.items, out_basename) catch |err| {
        std.log.err("compaction: day {s}/{s} completion failed post-commit: {} - manifest committed, recovery will finish on restart", .{ table.name(), date_str, err });
        return error.CompactionCommitIncomplete;
    };
    _ = malloc_trim(0);
}

/// Steps 5-7 of the protocol, shared by the live compaction and crash recovery:
/// delete each named input still present, rmdir emptied `hour=` dirs, publish the
/// `<seq>.building` to `<day_dir>/<out_basename>`, then drop `<seq>.manifest`.
/// The caller MUST hold LOCK_EX. Idempotent: re-running after a partial crash
/// (some inputs gone, building already published) completes cleanly.
///
/// Errors PROPAGATE so a failure leaves the committed manifest in place for the
/// next recoverCompactions to retry - we never delete the manifest (the recovery
/// marker) unless the merged file is durably published.
fn completeCompaction(allocator: Allocator, day_dir: []const u8, seq: []const u8, input_rels: []const []u8, out_basename: []const u8) !void {
    // The output line is durable recovery authority. Compaction ALWAYS writes
    // exactly `<seq>.parquet`, so require precisely that rather than a loose
    // basename shape: a parseable-but-malformed manifest naming a different
    // `other.parquet` would otherwise make recovery publish the building to the
    // wrong name (or, if that name already exists, drop the building + manifest,
    // losing the merged rows). Pinning it to `<seq>.parquet` closes that.
    const expected_out = try std.fmt.allocPrint(allocator, "{s}.parquet", .{seq});
    defer allocator.free(expected_out);
    if (!std.mem.eql(u8, out_basename, expected_out)) {
        std.log.warn("compaction completion: manifest output {s} is not the expected {s} - refusing", .{ out_basename, expected_out });
        return error.UnsafeManifestPath;
    }

    // A real compaction only commits when it merged >= 2 inputs, so a committed
    // manifest must list >= 2 inputs. A truncated-but-parseable manifest with 0-1
    // inputs (e.g. just the output line) would otherwise publish the building
    // while leaving the original inputs in place - a double-count. Treat it as a
    // corrupt committed manifest (fatal) rather than completing.
    if (input_rels.len < 2) {
        std.log.warn("compaction completion: manifest lists {d} inputs (<2) - corrupt, refusing", .{input_rels.len});
        return error.EmptyManifest;
    }

    // Every input the manifest authorizes us to DELETE must be a committed
    // `.parquet` (the only thing a compaction ever consumes) AND must not be the
    // output basename itself. Requiring the `.parquet` suffix rejects a parseable-
    // but-corrupt manifest that lists a reserved artifact - `<seq>.manifest`,
    // `<seq>.parquet.building`, or any non-parquet file. Rejecting an input equal
    // to `out_basename` prevents recovery (after a crash that left the final
    // published but the manifest present) from deleting the published final as an
    // "input" and then failing with no merged copy left. `.parquet.building` /
    // `.manifest` end in `.building`/`.manifest`, so they fail the suffix check.
    for (input_rels) |rel| {
        if (!std.mem.endsWith(u8, rel, ".parquet")) {
            std.log.warn("compaction completion: manifest input {s} is not a `.parquet` - refusing", .{rel});
            return error.UnsafeManifestPath;
        }
        if (std.mem.eql(u8, rel, out_basename)) {
            std.log.warn("compaction completion: manifest input {s} equals the output - refusing", .{rel});
            return error.UnsafeManifestPath;
        }
    }

    // (5) Delete each named input still present (FileNotFound tolerated so a
    // retried completion finishes), then rmdir emptied hour dirs.
    try deleteInputs(allocator, day_dir, input_rels);
    try removeEmptyHourDirs(allocator, day_dir);

    const building_path = try std.fmt.allocPrint(allocator, "{s}/{s}{s}", .{ day_dir, seq, building_suffix });
    defer allocator.free(building_path);
    const final_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ day_dir, out_basename });
    defer allocator.free(final_path);
    const manifest_path = try manifestPathOwned(allocator, day_dir, seq);
    defer allocator.free(manifest_path);

    // (6) Publish the building -> final, then fsync the dir. Probe the final with
    // a CHECKED existence: only a confirmed FileNotFound takes the "publish"
    // branch; any OTHER access error (EIO/EACCES/...) is propagated so we never
    // mistake an unreadable final for a published one and then drop the building +
    // manifest (which would lose rows). The manifest stays committed for retry.
    const final_exists = try pathExistsChecked(final_path);
    if (!final_exists) {
        fs.renameAbsolute(building_path, final_path) catch |err| switch (err) {
            error.FileNotFound => {
                // Neither building nor final present, yet the manifest committed
                // (so the inputs were scheduled for / already deleted). The merged
                // copy is gone - we must NOT drop the manifest (that would strand
                // the rows). Preserve the marker and fail so a restart can surface
                // the loss rather than silently completing. This is unreachable in
                // the protocol (the building is fsynced durable BEFORE the manifest
                // commits), so it only fires on out-of-band corruption.
                std.log.err("compaction completion: both building and final missing for {s} - manifest preserved", .{manifest_path});
                return error.CompactionOutputMissing;
            },
            else => return err,
        };
        try fsyncDirOf(final_path);
    } else {
        // Final already published (crash after step 6, before step 7): drop the
        // now-redundant leftover building, then fsync so the deletion is durable.
        fs.deleteFileAbsolute(building_path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        try fsyncDirOf(building_path);
    }

    // (7) Drop the manifest, then fsync the dir so the removal is DURABLE. The
    // manifest delete is part of the crash protocol: without the fsync a crash
    // here could resurrect the committed manifest, forcing recovery to replay an
    // already-complete compaction (and possibly hit CompactionOutputMissing once
    // the building is gone). Only reached after the merged file is durably
    // published, so the rows are never stranded.
    try fs.deleteFileAbsolute(manifest_path);
    try fsyncDirOf(manifest_path);
}

/// CONSERVATIVE existence probe: a non-FileNotFound access error reads as
/// "exists". Used by recoverDay's keep-the-building decision, where assuming a
/// manifest might exist is the SAFE choice (we keep the building rather than
/// risk deleting the only merged copy).
fn pathExists(path: []const u8) bool {
    fs.accessAbsolute(path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return true,
    };
    return true;
}

/// CHECKED existence probe: only a confirmed FileNotFound returns false; any
/// other access error PROPAGATES. Used by completeCompaction so an unreadable
/// final is never mistaken for a published one (which would drop the building +
/// manifest and lose rows) - the error preserves the committed manifest for retry.
fn pathExistsChecked(path: []const u8) !bool {
    fs.accessAbsolute(path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}

/// Recover compactions a crash interrupted, across every table's `date=` dir.
/// Call at daemon STARTUP under LOCK_EX, AFTER recoverOrphanTemps and BEFORE the
/// loop. Two cases per day dir:
///
///   - a `<seq>.manifest` exists (compaction was PAST its commit point): replay
///     steps 5-7 deterministically - delete any of the manifest's named inputs
///     still present, rmdir emptied `hour=` dirs, then publish `<seq>.building`
///     to `<seq>.parquet` (or drop the building if the final already exists),
///     then delete the manifest. This deletes EXACTLY the manifest's inputs -
///     never a concurrently-rolled file, never a prior merge.
///   - a `<seq>.building` WITHOUT a matching `.manifest` (crash BEFORE commit):
///     delete the building. Its inputs are all still present and intact.
///
/// Idempotent and safe when there is nothing to recover.
///
/// Takes LOCK_EX for the whole sweep, like compactDay/runRetention: recovery
/// mutates the tree (deletes inputs, publishes a `.building`), and a query holds
/// LOCK_SH around its enumerate+snapshot. Without the lock a query launched
/// concurrently with startup recovery could observe the inputs-deleted-but-
/// building-not-yet-published window and miss rows (queries ignore `.building`).
pub fn recoverCompactions(allocator: Allocator, root: []const u8) !void {
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

        var day_names = std.ArrayList([]u8){};
        defer {
            for (day_names.items) |d| allocator.free(d);
            day_names.deinit(allocator);
        }
        {
            defer dir.close();
            var it = dir.iterate();
            while (try it.next()) |entry| {
                if (entry.kind != .directory) continue;
                if (!std.mem.startsWith(u8, entry.name, "date=")) continue;
                try day_names.append(allocator, try allocator.dupe(u8, entry.name));
            }
        }

        for (day_names.items) |day_name| {
            const day_dir = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ table_dir, day_name });
            defer allocator.free(day_dir);
            try recoverDay(allocator, day_dir);
        }
    }
}

/// Recover one day dir: handle every `<seq>.manifest` (committed) and every
/// orphan `<seq>.building` without a manifest (pre-commit crash).
fn recoverDay(allocator: Allocator, day_dir: []const u8) !void {
    // Collect manifest + building basenames in a single non-recursive scan of
    // the day dir (manifests/buildings only ever sit at the day level).
    var manifests = std.ArrayList([]u8){};
    defer {
        for (manifests.items) |m| allocator.free(m);
        manifests.deinit(allocator);
    }
    var buildings = std.ArrayList([]u8){};
    defer {
        for (buildings.items) |b| allocator.free(b);
        buildings.deinit(allocator);
    }

    // Track whether we removed any pre-commit artifact so we can fsync the day
    // dir ONCE at the end - without it, a crash could resurrect a deleted
    // `.manifest.tmp`, which dayHasPendingCompaction would then defer on forever
    // until a later recovery durably removes it (a compaction-liveness bug).
    var cleaned = false;

    {
        var dir = fs.openDirAbsolute(day_dir, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer dir.close();
        var it = dir.iterate();
        while (try it.next()) |entry| {
            if (entry.kind != .file) continue;
            if (std.mem.endsWith(u8, entry.name, manifest_tmp_suffix)) {
                // An un-renamed manifest tmp is a pre-commit crash artifact: the
                // committed `.manifest` never appeared, so treat it like an
                // orphan and delete it (its building, if any, is handled below).
                const p = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ day_dir, entry.name });
                defer allocator.free(p);
                if (fs.deleteFileAbsolute(p)) |_| {
                    cleaned = true;
                } else |_| {}
            } else if (std.mem.endsWith(u8, entry.name, manifest_suffix)) {
                try manifests.append(allocator, try allocator.dupe(u8, entry.name));
            } else if (std.mem.endsWith(u8, entry.name, building_suffix)) {
                try buildings.append(allocator, try allocator.dupe(u8, entry.name));
            }
        }
    }

    // Committed manifests FIRST: each replays steps 5-7 (completeCompaction). On
    // success the manifest is dropped and its building consumed; on a parse error
    // BOTH the manifest and its building are intentionally LEFT in place (the day
    // stays un-compacted, inputs intact) for a human / a later release to inspect.
    for (manifests.items) |manifest_name| {
        const seq = manifest_name[0 .. manifest_name.len - manifest_suffix.len];
        try recoverFromManifest(allocator, day_dir, seq);
    }

    // Remaining buildings: a building is an ORPHAN (crash BEFORE the manifest
    // commit) ONLY if no `<seq>.manifest` exists for it. Delete only those - their
    // inputs were never touched, so dropping the building loses nothing. A
    // building whose manifest STILL exists (an unparseable manifest left in place
    // above) is the only complete merged copy and MUST be kept, so we re-check the
    // manifest on disk per building rather than blindly deleting every one.
    for (buildings.items) |building_name| {
        const seq = building_name[0 .. building_name.len - building_suffix.len];
        const manifest_path = try manifestPathOwned(allocator, day_dir, seq);
        defer allocator.free(manifest_path);
        if (pathExists(manifest_path)) continue; // committed (e.g. unparseable) - keep the building
        const p = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ day_dir, building_name });
        defer allocator.free(p);
        if (fs.deleteFileAbsolute(p)) |_| {
            cleaned = true;
        } else |_| {}
    }

    // Make the pre-commit-artifact removals durable (see `cleaned` above) by
    // fsyncing the day dir itself. Best-effort: a transient fsync error here only
    // costs a redundant re-cleanup on the next startup, never correctness.
    if (cleaned) fsyncDir(day_dir) catch {};
}

/// Replay steps 5-7 for a committed `<seq>.manifest`: delete the listed inputs
/// still present, rmdir emptied `hour=` dirs, publish the building (or drop it if
/// the final already exists), then delete the manifest.
fn recoverFromManifest(allocator: Allocator, day_dir: []const u8, seq: []const u8) !void {
    const manifest_path = try manifestPathOwned(allocator, day_dir, seq);
    defer allocator.free(manifest_path);

    // A committed manifest is the recovery AUTHORITY: a crash may have already
    // deleted some of its inputs, so we cannot safely "leave the day uncompacted"
    // if we can't read it - those rows might survive ONLY in the building. A
    // truncated/garbage/empty committed manifest is therefore FATAL (propagated):
    // startup aborts, the manifest + building are preserved on disk, and a human
    // can inspect rather than the daemon silently serving a short count. (The
    // building itself stays put: recoverDay's orphan loop skips any building whose
    // manifest still exists.)
    var entries = parseManifest(allocator, manifest_path) catch |err| {
        // Logged at warn (the FATAL signal is the propagated error + main.zig's
        // abort, not the log level); err-level here would also trip the test
        // runner's "logged errors" check on the deliberate fatal-path test.
        std.log.warn("compaction recovery: cannot parse committed manifest {s}: {} - aborting (preserved for inspection)", .{ manifest_path, err });
        return err;
    };
    defer {
        for (entries.items) |e| allocator.free(e);
        entries.deinit(allocator);
    }

    // Manifest layout: line 0 = output basename, lines 1.. = input rel paths.
    if (entries.items.len == 0) {
        std.log.warn("compaction recovery: empty committed manifest {s} - aborting (preserved for inspection)", .{manifest_path});
        return error.EmptyManifest;
    }
    const out_basename = entries.items[0];
    const input_rels = entries.items[1..];

    // Replay steps 5-7 via the SAME completion path the live compaction uses.
    // It deletes the named inputs, publishes the building, and drops the manifest;
    // on a missing merged copy it PRESERVES the manifest and errors (propagated)
    // rather than silently completing - so recovery never strands rows.
    try completeCompaction(allocator, day_dir, seq, input_rels, out_basename);
}

/// Enumerate every committed `.parquet` under `day_dir`, recursively, as paths
/// RELATIVE to `day_dir`. Skips in-flight `.parquet.tmp` (a roll temp) and the
/// `.building` (a prior interrupted compaction's output) so neither is consumed
/// as an input. Caller frees each item + the list.
fn collectDayInputs(allocator: Allocator, day_dir: []const u8) !std.ArrayList([]u8) {
    var out = std.ArrayList([]u8){};
    errdefer {
        for (out.items) |p| allocator.free(p);
        out.deinit(allocator);
    }

    var dir = fs.openDirAbsolute(day_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return out,
        else => return err,
    };
    defer dir.close();

    var walker = try dir.walk(allocator);
    defer walker.deinit();
    while (try walker.next()) |entry| {
        if (entry.kind != .file) continue;
        // Match ONLY committed `.parquet`. `.parquet.tmp` and `.building` both
        // fail endsWith(".parquet") in a way that excludes them: `.parquet.tmp`
        // ends in ".tmp" and `.building` ends in ".building". Be explicit.
        if (!std.mem.endsWith(u8, entry.basename, ".parquet")) continue;
        try out.append(allocator, try allocator.dupe(u8, entry.path));
    }
    return out;
}

/// COPY the union of `inputs` (paths relative to `day_dir`) into `out_path` as a
/// single zstd parquet, via a transient in-memory DuckDB. read_parquet reads the
/// explicit absolute-path list (NOT a glob) so exactly the named inputs merge.
fn copyMergeToParquet(allocator: Allocator, day_dir: []const u8, inputs: []const []u8, out_path: []const u8) !void {
    var db: c.duckdb_database = undefined;
    if (c.duckdb_open(":memory:", &db) == c.DuckDBError) return RollError.DatabaseError;
    defer c.duckdb_close(&db);
    var conn: c.duckdb_connection = undefined;
    if (c.duckdb_connect(db, &conn) == c.DuckDBError) return RollError.ConnectionError;
    defer c.duckdb_disconnect(&conn);

    // Build the escaped, single-quoted, comma-separated absolute-path list.
    var list = std.ArrayList(u8){};
    defer list.deinit(allocator);
    for (inputs, 0..) |rel, idx| {
        if (idx != 0) try list.append(allocator, ',');
        const abs = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ day_dir, rel });
        defer allocator.free(abs);
        const escaped = try escapeSqlLiteral(allocator, abs);
        defer allocator.free(escaped);
        try list.append(allocator, '\'');
        try list.appendSlice(allocator, escaped);
        try list.append(allocator, '\'');
    }

    const escaped_out = try escapeSqlLiteral(allocator, out_path);
    defer allocator.free(escaped_out);

    // hive_partitioning=false: the input set can mix depths - a day-level
    // <seq>.parquet (date key only) from a prior merge alongside hour-level files
    // (date+hour keys) if an old-timestamp roll landed in a hour leaf after a
    // compaction. hive=true errors on that inconsistent depth (the same reason
    // the query path disables it); we SELECT * and never read the path
    // components as columns, so hive is irrelevant to the merge. union_by_name=
    // true reconciles any column-order/schema drift across inputs by name. The
    // output is a single file.
    const sql = try std.fmt.allocPrintSentinel(
        allocator,
        "COPY (SELECT * FROM read_parquet([{s}], hive_partitioning=false, union_by_name=true)) TO '{s}' (FORMAT parquet, COMPRESSION zstd)",
        .{ list.items, escaped_out },
        0,
    );
    defer allocator.free(sql);

    var result: c.duckdb_result = undefined;
    const state = c.duckdb_query(conn, sql.ptr, &result);
    defer c.duckdb_destroy_result(&result);
    if (state == c.DuckDBError) {
        std.log.err("compaction COPY merge error: {s}", .{c.duckdb_result_error(&result)});
        return RollError.CopyError;
    }
    _ = malloc_trim(0);
}

/// Write `<day_dir>/<seq>.manifest.tmp` (output basename on line 0, each input
/// rel path on its own line), fsync it, then atomic-rename to
/// `<day_dir>/<seq>.manifest` and fsync the dir. The rename is the COMMIT POINT.
fn writeManifest(allocator: Allocator, day_dir: []const u8, seq: []const u8, final_path: []const u8, inputs: []const []u8) !void {
    const out_basename = std.fs.path.basename(final_path);

    // Refuse to commit an AMBIGUOUS manifest: an input whose name is not a safe
    // rel path (e.g. contains a newline, which the newline-delimited format would
    // split into bogus lines, making recovery delete the wrong paths). This
    // aborts the compaction PRE-COMMIT (best-effort: the day stays untouched)
    // rather than ever writing a manifest recovery could misread.
    for (inputs) |rel| {
        if (!isSafeRelPath(rel)) {
            std.log.warn("compaction: input {s} is not a safe manifest rel path - skipping day", .{rel});
            return error.UnsafeManifestPath;
        }
    }

    var body = std.ArrayList(u8){};
    defer body.deinit(allocator);
    try body.appendSlice(allocator, out_basename);
    try body.append(allocator, '\n');
    for (inputs) |rel| {
        try body.appendSlice(allocator, rel);
        try body.append(allocator, '\n');
    }

    const tmp_path = try std.fmt.allocPrint(allocator, "{s}/{s}{s}", .{ day_dir, seq, manifest_tmp_suffix });
    defer allocator.free(tmp_path);
    const manifest_path = try manifestPathOwned(allocator, day_dir, seq);
    defer allocator.free(manifest_path);

    {
        const f = try fs.createFileAbsolute(tmp_path, .{ .mode = 0o600 });
        defer f.close();
        try f.writeAll(body.items);
        try f.sync();
    }
    try fs.renameAbsolute(tmp_path, manifest_path); // COMMIT POINT
    try fsyncDirOf(manifest_path);
}

/// Parse a manifest into its newline-separated entries (line 0 = output
/// basename, the rest = input rel paths). Rejects any entry with a leading `/`
/// or a `..` path component so a tampered manifest cannot escape the day dir on
/// delete. Caller frees each item + the list.
fn parseManifest(allocator: Allocator, manifest_path: []const u8) !std.ArrayList([]u8) {
    var out = std.ArrayList([]u8){};
    errdefer {
        for (out.items) |e| allocator.free(e);
        out.deinit(allocator);
    }

    const f = try fs.openFileAbsolute(manifest_path, .{});
    defer f.close();
    // A manifest lists a bounded number of small rel paths; cap the read.
    const data = try f.readToEndAlloc(allocator, 8 * 1024 * 1024);
    defer allocator.free(data);

    var lines = std.mem.splitScalar(u8, data, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        if (!isSafeRelPath(line)) return error.UnsafeManifestPath;
        try out.append(allocator, try allocator.dupe(u8, line));
    }
    return out;
}

/// A rel path is safe to join under the day dir (via "{day_dir}/{rel}") iff it is
/// relative and every component is a plain name. Guards manifest-driven deletes:
/// rejects a leading `/`, any `..` component, any EMPTY component (a leading,
/// trailing, or doubled `/` like `hour=00//x.parquet`), and any backslash
/// (defensive against a path-syntax surprise). The daemon only ever writes
/// `hour=HH/<seq>.parquet` or `<seq>.parquet`, so a real manifest always passes.
fn isSafeRelPath(p: []const u8) bool {
    if (p.len == 0) return false;
    if (p[0] == '/') return false;
    if (std.mem.indexOfScalar(u8, p, '\\') != null) return false;
    // Reject ANY control char (< 0x20). The manifest is newline-delimited, so an
    // embedded '\n'/'\r' in a filename would be split into bogus lines and make
    // recovery delete the wrong paths (a double-count risk). Rejecting all
    // controls is stricter than necessary but never excludes a real input (the
    // daemon only writes `hour=HH/<seq>.parquet` / `<seq>.parquet`).
    for (p) |ch| if (ch < 0x20) return false;
    var it = std.mem.splitScalar(u8, p, '/');
    while (it.next()) |comp| {
        if (comp.len == 0) return false; // empty component (leading/trailing/doubled `/`)
        if (std.mem.eql(u8, comp, "..")) return false;
        if (std.mem.eql(u8, comp, ".")) return false;
    }
    return true;
}

/// Delete each input (rel path) under `day_dir`, then fsync every distinct parent
/// directory the deletions touched so the UNLINKS ARE DURABLE before the caller
/// proceeds to delete the manifest. Without this, an input deleted from a
/// `hour=*` subdir might NOT be persisted (only the day dir is fsynced later); a
/// crash after the manifest removal could then resurrect that input alongside the
/// published final - a double-count with no manifest left to clean it up. A
/// missing file is tolerated so a retried completion (some inputs already gone)
/// still finishes.
fn deleteInputs(allocator: Allocator, day_dir: []const u8, inputs: []const []u8) !void {
    // Track distinct parent dirs to fsync once each (typically the few hour= dirs).
    var parents = std.ArrayList([]u8){};
    defer {
        for (parents.items) |p| allocator.free(p);
        parents.deinit(allocator);
    }

    for (inputs) |rel| {
        const p = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ day_dir, rel });
        defer allocator.free(p);
        fs.deleteFileAbsolute(p) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        const parent = std.fs.path.dirname(p) orelse day_dir;
        var seen = false;
        for (parents.items) |existing| {
            if (std.mem.eql(u8, existing, parent)) {
                seen = true;
                break;
            }
        }
        if (!seen) try parents.append(allocator, try allocator.dupe(u8, parent));
    }

    // Make the unlinks durable. EINVAL-tolerant; a parent already rmdir'd by a
    // concurrent step reads as FileNotFound, which we tolerate.
    for (parents.items) |parent| {
        fsyncDir(parent) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    }
}

/// Remove every now-empty `hour=*` subdir of `day_dir` (best-effort: a non-empty
/// dir or a transient error is skipped). Called after the day's inputs are
/// deleted so the merged day-level file is the only content left.
fn removeEmptyHourDirs(allocator: Allocator, day_dir: []const u8) !void {
    var dir = fs.openDirAbsolute(day_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };

    var hour_dirs = std.ArrayList([]u8){};
    defer {
        for (hour_dirs.items) |h| allocator.free(h);
        hour_dirs.deinit(allocator);
    }
    {
        defer dir.close();
        var it = dir.iterate();
        while (try it.next()) |entry| {
            if (entry.kind != .directory) continue;
            if (!std.mem.startsWith(u8, entry.name, "hour=")) continue;
            try hour_dirs.append(allocator, try allocator.dupe(u8, entry.name));
        }
    }

    for (hour_dirs.items) |hour_name| {
        const p = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ day_dir, hour_name });
        defer allocator.free(p);
        // deleteDirAbsolute removes only an EMPTY dir; a stray non-parquet file
        // would make it ENOTEMPTY, which we skip rather than blow away data.
        fs.deleteDirAbsolute(p) catch {};
    }
}

/// `<day_dir>/<seq>.manifest`. Caller frees.
fn manifestPathOwned(allocator: Allocator, day_dir: []const u8, seq: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}/{s}{s}", .{ day_dir, seq, manifest_suffix });
}

/// True iff `day_dir` holds any unfinished-compaction artifact at the day level -
/// a `.manifest`, a `.manifest.tmp`, or a `.building`. A fresh compactDay must
/// not start over such a day (see compactDay step 0): only recoverCompactions
/// may resolve it, so re-merging a partial input set can't create a second
/// manifest that double-counts on the next startup.
fn dayHasPendingCompaction(day_dir: []const u8) !bool {
    var dir = fs.openDirAbsolute(day_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer dir.close();
    var it = dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind != .file) continue;
        if (std.mem.endsWith(u8, entry.name, manifest_suffix) or
            std.mem.endsWith(u8, entry.name, manifest_tmp_suffix) or
            std.mem.endsWith(u8, entry.name, building_suffix)) return true;
    }
    return false;
}

/// Stable hex hash over the SORTED input rel paths (NUL-joined so distinct
/// path sets cannot collide via concatenation). Deterministic: the same input
/// set always yields the same seq, so a retried compaction reuses one output name.
fn seqFromRelPaths(allocator: Allocator, sorted_rels: []const []u8) ![]u8 {
    var hasher = std.hash.Wyhash.init(0);
    for (sorted_rels) |rel| {
        hasher.update(rel);
        hasher.update(&[_]u8{0});
    }
    return std.fmt.allocPrint(allocator, "{x:0>16}", .{hasher.final()});
}

fn lessThanSlice(_: void, a: []u8, b: []u8) bool {
    return std.mem.lessThan(u8, a, b);
}

/// Days since the unix epoch for a `YYYY-MM-DD` partition NAME, or null if
/// malformed. Reuses dayStartUnix's calendar math (its result / s_per_day).
fn dayIndexFromName(date_str: []const u8) ?i64 {
    const start = dayStartUnix(date_str) orelse return null;
    return @divFloor(start, std.time.s_per_day);
}

/// Today's date as days-since-epoch (UTC), for the sealed-day guard.
fn todayUtcDays() i64 {
    return @divFloor(std.time.timestamp(), std.time.s_per_day);
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
    // migration is idempotent and never double-counts. collectParquetFiles walks
    // the whole table tree so this file is included; the query reads its explicit
    // data columns (hive_partitioning is off), so the absent date=/hour= path
    // components don't matter - queries filter on the real `timestamp` column.
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

// ----------------------------------------------------------------------------
// Day-compaction tests (crash-injection at each protocol phase)
// ----------------------------------------------------------------------------

/// A fixed SEALED day far enough in the past that the sealed guard (date < today
/// UTC) always holds, plus its days-since-epoch parts for building hour leaves.
const test_sealed_date = "2023-11-14"; // 1_700_000_000 lands here (UTC)

/// Build `<root>/metrics/date=<test_sealed_date>/hour=<HH>/<seq>.parquet` for a
/// fresh hour leaf, each holding `rows` metrics rows, via a real roll then a move
/// into the chosen hour dir. Returns nothing; total rowcount is rows*hours.
fn seedSealedDay(a: Allocator, root: []const u8, hours: usize, rows_per_hour: usize) !void {
    // The roll partitions by the row timestamp; 1_700_000_000 is in date=2023-11-14
    // hour=22 UTC. We don't depend on which hour the roll picks - after producing
    // each file we relocate it into a distinct hour leaf so the day has N leaves.
    var h: usize = 0;
    while (h < hours) : (h += 1) {
        var stg = try staging.Staging.open(a, root);
        defer stg.deinit();
        var r: usize = 0;
        while (r < rows_per_hour) : (r += 1) {
            // Distinct timestamps per file so dedup checks are meaningful.
            const ts: i64 = 1_700_000_000 + @as(i64, @intCast(h)) * 100 + @as(i64, @intCast(r));
            try stg.appendMetrics(ts, sampleMetrics());
        }
        try stg.sync();
        const res = (try rollTable(a, root, &stg, .metrics)).?;
        defer a.free(res.parquet_path);

        // Relocate into a deterministic hour leaf for this file.
        const hour_dir = try std.fmt.allocPrint(a, "{s}/metrics/date={s}/hour={d:0>2}", .{ root, test_sealed_date, h });
        defer a.free(hour_dir);
        try makePathAbsolute(hour_dir);
        const base = std.fs.path.basename(res.parquet_path);
        const dest = try std.fmt.allocPrint(a, "{s}/{s}", .{ hour_dir, base });
        defer a.free(dest);
        try fs.renameAbsolute(res.parquet_path, dest);
    }
}

/// Count all rows under `<root>/metrics/date=<test_sealed_date>/**` plus the
/// day-level file, via the same read_parquet path the query uses.
fn dayRowCount(a: Allocator, root: []const u8) !i64 {
    const glob = try std.fmt.allocPrint(a, "{s}/metrics/date={s}/**/*.parquet", .{ root, test_sealed_date });
    defer a.free(glob);
    var db: c.duckdb_database = undefined;
    try testing.expect(c.duckdb_open(":memory:", &db) != c.DuckDBError);
    defer c.duckdb_close(&db);
    var conn: c.duckdb_connection = undefined;
    try testing.expect(c.duckdb_connect(db, &conn) != c.DuckDBError);
    defer c.duckdb_disconnect(&conn);
    const sql = try std.fmt.allocPrintSentinel(a, "SELECT COUNT(*) FROM read_parquet('{s}', hive_partitioning=false, union_by_name=true)", .{glob}, 0);
    defer a.free(sql);
    var result: c.duckdb_result = undefined;
    try testing.expect(c.duckdb_query(conn, sql.ptr, &result) != c.DuckDBError);
    defer c.duckdb_destroy_result(&result);
    return c.duckdb_value_int64(&result, 0, 0);
}

fn dayDirPath(a: Allocator, root: []const u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}/metrics/date={s}", .{ root, test_sealed_date });
}

/// Count `.parquet` files directly under a path that is NOT under an hour= dir
/// (the day level) plus those under hour=* dirs. Returns {day_level, in_hours}.
fn countDayFiles(a: Allocator, root: []const u8) !struct { day_level: usize, in_hours: usize } {
    const day_dir = try dayDirPath(a, root);
    defer a.free(day_dir);
    var dir = try fs.openDirAbsolute(day_dir, .{ .iterate = true });
    defer dir.close();
    var day_level: usize = 0;
    var in_hours: usize = 0;
    var walker = try dir.walk(a);
    defer walker.deinit();
    while (try walker.next()) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.basename, ".parquet")) continue;
        if (std.mem.indexOf(u8, entry.path, "hour=") != null) in_hours += 1 else day_level += 1;
    }
    return .{ .day_level = day_level, .in_hours = in_hours };
}

test "compaction: happy path merges many hour files into one day file, same rowcount" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    try seedSealedDay(a, root, 4, 3); // 4 hour leaves, 3 rows each = 12
    try testing.expectEqual(@as(i64, 12), try dayRowCount(a, root));
    const before = try countDayFiles(a, root);
    try testing.expectEqual(@as(usize, 4), before.in_hours);
    try testing.expectEqual(@as(usize, 0), before.day_level);

    try compactDay(a, root, .metrics, test_sealed_date);

    // Exactly one DAY-LEVEL file, no hour-level files, and the hour= dirs are gone.
    const after = try countDayFiles(a, root);
    try testing.expectEqual(@as(usize, 1), after.day_level);
    try testing.expectEqual(@as(usize, 0), after.in_hours);
    // Rowcount unchanged: no dup, no loss.
    try testing.expectEqual(@as(i64, 12), try dayRowCount(a, root));
}

test "compaction: idempotent re-run is a no-op (single file already)" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    try seedSealedDay(a, root, 3, 2); // 6 rows
    try compactDay(a, root, .metrics, test_sealed_date);
    try testing.expectEqual(@as(i64, 6), try dayRowCount(a, root));

    // Second run: only one day-level file remains (|I| <= 1) so it's a no-op.
    try compactDay(a, root, .metrics, test_sealed_date);
    const after = try countDayFiles(a, root);
    try testing.expectEqual(@as(usize, 1), after.day_level);
    try testing.expectEqual(@as(i64, 6), try dayRowCount(a, root));
}

test "compaction: sealed-only guard - today's day dir is never compacted" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    // Build TWO hour leaves under TODAY's date via a normal roll at `now`.
    const now = std.time.timestamp();
    {
        var stg = try staging.Staging.open(a, root);
        defer stg.deinit();
        try stg.appendMetrics(now, sampleMetrics());
        try stg.sync();
        const r1 = (try rollTable(a, root, &stg, .metrics)).?;
        a.free(r1.parquet_path);
        try stg.appendMetrics(now + 1, sampleMetrics());
        try stg.sync();
        const r2 = (try rollTable(a, root, &stg, .metrics)).?;
        a.free(r2.parquet_path);
    }

    // compactSealedDays must skip today: its hour leaves stay intact.
    try compactSealedDays(a, root);

    const today_str = blk: {
        const ts: u64 = @intCast(now);
        const ep = std.time.epoch.EpochSeconds{ .secs = ts };
        const yd = ep.getEpochDay().calculateYearDay();
        const md = yd.calculateMonthDay();
        break :blk try std.fmt.allocPrint(a, "{d:0>4}-{d:0>2}-{d:0>2}", .{ yd.year, md.month.numeric(), md.day_index + 1 });
    };
    defer a.free(today_str);

    const day_dir = try std.fmt.allocPrint(a, "{s}/metrics/date={s}", .{ root, today_str });
    defer a.free(day_dir);
    var inputs = try collectDayInputs(a, day_dir);
    defer {
        for (inputs.items) |p| a.free(p);
        inputs.deinit(a);
    }
    // Both hour-level files survive: today was not touched.
    try testing.expectEqual(@as(usize, 2), inputs.items.len);
}

test "compaction recovery (a): orphan .building, no manifest -> dropped, inputs intact" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    try seedSealedDay(a, root, 3, 2); // 6 rows across 3 hour leaves
    const day_dir = try dayDirPath(a, root);
    defer a.free(day_dir);

    // Inject a pre-commit crash: a `<seq>.building` with NO manifest. We make a
    // real merged file (so it has real bytes) but place it as `.building`.
    var inputs = try collectDayInputs(a, day_dir);
    defer {
        for (inputs.items) |p| a.free(p);
        inputs.deinit(a);
    }
    std.mem.sort([]u8, inputs.items, {}, lessThanSlice);
    const seq = try seqFromRelPaths(a, inputs.items);
    defer a.free(seq);
    const building = try std.fmt.allocPrint(a, "{s}/{s}{s}", .{ day_dir, seq, building_suffix });
    defer a.free(building);
    try copyMergeToParquet(a, day_dir, inputs.items, building);

    // Recovery must DROP the building and leave every input intact: rowcount 6.
    try recoverCompactions(a, root);
    try testing.expectError(error.FileNotFound, fs.accessAbsolute(building, .{}));
    try testing.expectEqual(@as(i64, 6), try dayRowCount(a, root));
    // Inputs still in their hour leaves (un-compacted).
    const after = try countDayFiles(a, root);
    try testing.expectEqual(@as(usize, 3), after.in_hours);
    try testing.expectEqual(@as(usize, 0), after.day_level);
}

test "compaction recovery (b): manifest committed, all inputs + building present -> publish, no dup" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    try seedSealedDay(a, root, 4, 2); // 8 rows
    const day_dir = try dayDirPath(a, root);
    defer a.free(day_dir);

    var inputs = try collectDayInputs(a, day_dir);
    defer {
        for (inputs.items) |p| a.free(p);
        inputs.deinit(a);
    }
    std.mem.sort([]u8, inputs.items, {}, lessThanSlice);
    const seq = try seqFromRelPaths(a, inputs.items);
    defer a.free(seq);
    const building = try std.fmt.allocPrint(a, "{s}/{s}{s}", .{ day_dir, seq, building_suffix });
    defer a.free(building);
    const final = try std.fmt.allocPrint(a, "{s}/{s}.parquet", .{ day_dir, seq });
    defer a.free(final);

    // Inject the post-commit state: building written, manifest committed, but the
    // inputs NOT yet deleted and the building NOT yet renamed (crash in step 5).
    try copyMergeToParquet(a, day_dir, inputs.items, building);
    try writeManifest(a, day_dir, seq, final, inputs.items);

    try recoverCompactions(a, root);

    // Recovery deletes the named inputs and publishes the building: one day-level
    // file, no hour files, rowcount unchanged at 8 (no double-count).
    const after = try countDayFiles(a, root);
    try testing.expectEqual(@as(usize, 1), after.day_level);
    try testing.expectEqual(@as(usize, 0), after.in_hours);
    try testing.expectEqual(@as(i64, 8), try dayRowCount(a, root));
}

test "compaction recovery (c): manifest committed, SOME inputs already deleted -> completes, no dup" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    try seedSealedDay(a, root, 4, 2); // 8 rows
    const day_dir = try dayDirPath(a, root);
    defer a.free(day_dir);

    var inputs = try collectDayInputs(a, day_dir);
    defer {
        for (inputs.items) |p| a.free(p);
        inputs.deinit(a);
    }
    std.mem.sort([]u8, inputs.items, {}, lessThanSlice);
    const seq = try seqFromRelPaths(a, inputs.items);
    defer a.free(seq);
    const building = try std.fmt.allocPrint(a, "{s}/{s}{s}", .{ day_dir, seq, building_suffix });
    defer a.free(building);
    const final = try std.fmt.allocPrint(a, "{s}/{s}.parquet", .{ day_dir, seq });
    defer a.free(final);

    try copyMergeToParquet(a, day_dir, inputs.items, building);
    try writeManifest(a, day_dir, seq, final, inputs.items);

    // Simulate a crash PARTWAY through step 5: delete the FIRST input only.
    {
        const p = try std.fmt.allocPrint(a, "{s}/{s}", .{ day_dir, inputs.items[0] });
        defer a.free(p);
        try fs.deleteFileAbsolute(p);
    }

    try recoverCompactions(a, root);

    // Recovery deletes the remaining inputs and publishes: exactly one day file,
    // rowcount unchanged at 8 (the building holds all 8; no dup, no loss).
    const after = try countDayFiles(a, root);
    try testing.expectEqual(@as(usize, 1), after.day_level);
    try testing.expectEqual(@as(usize, 0), after.in_hours);
    try testing.expectEqual(@as(i64, 8), try dayRowCount(a, root));
}

test "compaction recovery (d): manifest present, inputs deleted, building already renamed -> drop manifest" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    try seedSealedDay(a, root, 3, 2); // 6 rows
    const day_dir = try dayDirPath(a, root);
    defer a.free(day_dir);

    var inputs = try collectDayInputs(a, day_dir);
    defer {
        for (inputs.items) |p| a.free(p);
        inputs.deinit(a);
    }
    std.mem.sort([]u8, inputs.items, {}, lessThanSlice);
    const seq = try seqFromRelPaths(a, inputs.items);
    defer a.free(seq);
    const final = try std.fmt.allocPrint(a, "{s}/{s}.parquet", .{ day_dir, seq });
    defer a.free(final);

    // Inject the near-complete state: final already published, inputs deleted,
    // hour dirs removed, but the manifest NOT yet dropped (crash between 6 and 7).
    try copyMergeToParquet(a, day_dir, inputs.items, final);
    try writeManifest(a, day_dir, seq, final, inputs.items);
    try deleteInputs(a, day_dir, inputs.items);
    try removeEmptyHourDirs(a, day_dir);

    const manifest_path = try manifestPathOwned(a, day_dir, seq);
    defer a.free(manifest_path);
    try fs.accessAbsolute(manifest_path, .{}); // manifest is present pre-recovery

    try recoverCompactions(a, root);

    // Recovery just drops the manifest; the final stays, rowcount unchanged at 6.
    try testing.expectError(error.FileNotFound, fs.accessAbsolute(manifest_path, .{}));
    const after = try countDayFiles(a, root);
    try testing.expectEqual(@as(usize, 1), after.day_level);
    try testing.expectEqual(@as(i64, 6), try dayRowCount(a, root));
}

test "compaction: pending artifact defers a fresh compactDay (no second manifest -> no later double-count)" {
    // Codex BLOCKER: if compactDay ran over a day that already had a committed-
    // but-unfinished manifest, it would write a SECOND manifest/building over a
    // now-partial input set, and the next startup recovery would publish BOTH the
    // old (full) building and the new (partial) one - double-counting. compactDay
    // must DEFER such a day to recovery; here we inject a committed manifest +
    // building, then assert compactDay leaves them untouched (no extra artifacts).
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    try seedSealedDay(a, root, 4, 2); // 8 rows
    const day_dir = try dayDirPath(a, root);
    defer a.free(day_dir);

    var inputs = try collectDayInputs(a, day_dir);
    defer {
        for (inputs.items) |p| a.free(p);
        inputs.deinit(a);
    }
    std.mem.sort([]u8, inputs.items, {}, lessThanSlice);
    const seq = try seqFromRelPaths(a, inputs.items);
    defer a.free(seq);
    const building = try std.fmt.allocPrint(a, "{s}/{s}{s}", .{ day_dir, seq, building_suffix });
    defer a.free(building);
    const final = try std.fmt.allocPrint(a, "{s}/{s}.parquet", .{ day_dir, seq });
    defer a.free(final);
    try copyMergeToParquet(a, day_dir, inputs.items, building);
    try writeManifest(a, day_dir, seq, final, inputs.items);

    // compactDay must DEFER (the pending manifest blocks it): the inputs and the
    // single committed manifest/building survive untouched - no NEW seq artifact.
    try compactDay(a, root, .metrics, test_sealed_date);
    var n_manifests: usize = 0;
    var n_buildings: usize = 0;
    {
        var dir = try fs.openDirAbsolute(day_dir, .{ .iterate = true });
        defer dir.close();
        var it = dir.iterate();
        while (try it.next()) |e| {
            if (e.kind != .file) continue;
            if (std.mem.endsWith(u8, e.name, manifest_suffix)) n_manifests += 1;
            if (std.mem.endsWith(u8, e.name, building_suffix)) n_buildings += 1;
        }
    }
    try testing.expectEqual(@as(usize, 1), n_manifests); // only the injected one
    try testing.expectEqual(@as(usize, 1), n_buildings);

    // And recovery still completes the original compaction to exactly 8 rows.
    try recoverCompactions(a, root);
    try testing.expectEqual(@as(i64, 8), try dayRowCount(a, root));
}

test "compaction recovery: an unparseable committed manifest is FATAL and preserves its building + inputs" {
    // Codex HIGH: a committed manifest is recovery authority - a crash may have
    // already deleted some of its inputs, so an unreadable one cannot be safely
    // skipped. Recovery must ABORT (propagate the error) and leave the manifest +
    // building + every input on disk for inspection - never silently proceed.
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    try seedSealedDay(a, root, 3, 2); // 6 rows
    const day_dir = try dayDirPath(a, root);
    defer a.free(day_dir);

    var inputs = try collectDayInputs(a, day_dir);
    defer {
        for (inputs.items) |p| a.free(p);
        inputs.deinit(a);
    }
    std.mem.sort([]u8, inputs.items, {}, lessThanSlice);
    const seq = try seqFromRelPaths(a, inputs.items);
    defer a.free(seq);
    const building = try std.fmt.allocPrint(a, "{s}/{s}{s}", .{ day_dir, seq, building_suffix });
    defer a.free(building);
    try copyMergeToParquet(a, day_dir, inputs.items, building);

    // Write a CORRUPT committed manifest (an unsafe `..` path -> parseManifest
    // returns error.UnsafeManifestPath, which recovery propagates as fatal).
    const manifest_path = try manifestPathOwned(a, day_dir, seq);
    defer a.free(manifest_path);
    {
        const f = try fs.createFileAbsolute(manifest_path, .{ .mode = 0o600 });
        defer f.close();
        try f.writeAll("out.parquet\n../escape.parquet\n");
    }

    // Recovery ABORTS rather than completing or skipping.
    try testing.expectError(error.UnsafeManifestPath, recoverCompactions(a, root));

    // Manifest, building, and all original inputs survive: nothing lost, the day
    // is left exactly as the crash left it for a human / a later release.
    try fs.accessAbsolute(manifest_path, .{});
    try fs.accessAbsolute(building, .{});
    try testing.expectEqual(@as(i64, 6), try dayRowCount(a, root));
}

test "compaction recovery: a parseable manifest with the wrong output name is refused, building kept" {
    // Codex round-3 HIGH: a parseable-but-malformed committed manifest naming a
    // DIFFERENT output (other.parquet, not <seq>.parquet) must be refused so
    // recovery can't publish/drop the building to the wrong name and lose rows.
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    try seedSealedDay(a, root, 3, 2); // 6 rows
    const day_dir = try dayDirPath(a, root);
    defer a.free(day_dir);

    var inputs = try collectDayInputs(a, day_dir);
    defer {
        for (inputs.items) |p| a.free(p);
        inputs.deinit(a);
    }
    std.mem.sort([]u8, inputs.items, {}, lessThanSlice);
    const seq = try seqFromRelPaths(a, inputs.items);
    defer a.free(seq);
    const building = try std.fmt.allocPrint(a, "{s}/{s}{s}", .{ day_dir, seq, building_suffix });
    defer a.free(building);
    try copyMergeToParquet(a, day_dir, inputs.items, building);

    // Committed manifest whose output line is a VALID-shaped basename but NOT
    // <seq>.parquet. Inputs are the real rel paths so parse succeeds.
    const manifest_path = try manifestPathOwned(a, day_dir, seq);
    defer a.free(manifest_path);
    {
        var body = std.ArrayList(u8){};
        defer body.deinit(a);
        try body.appendSlice(a, "other.parquet\n");
        for (inputs.items) |rel| {
            try body.appendSlice(a, rel);
            try body.append(a, '\n');
        }
        const f = try fs.createFileAbsolute(manifest_path, .{ .mode = 0o600 });
        defer f.close();
        try f.writeAll(body.items);
    }

    // Recovery refuses (fatal) - it does not publish to other.parquet or drop the
    // building. Manifest + building survive; no rows lost.
    try testing.expectError(error.UnsafeManifestPath, recoverCompactions(a, root));
    try fs.accessAbsolute(manifest_path, .{});
    try fs.accessAbsolute(building, .{});
    try testing.expectEqual(@as(i64, 6), try dayRowCount(a, root));
    const wrong = try std.fmt.allocPrint(a, "{s}/other.parquet", .{day_dir});
    defer a.free(wrong);
    try testing.expectError(error.FileNotFound, fs.accessAbsolute(wrong, .{}));
}

test "compaction recovery: a manifest listing a non-parquet input is refused, nothing deleted" {
    // Codex round-4 HIGH: input lines are delete authority, so each must be a
    // committed `.parquet`. A parseable manifest naming a reserved artifact (here
    // the building itself) must be refused BEFORE any delete, so recovery can't
    // unlink its own marker/building or an unrelated file.
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    try seedSealedDay(a, root, 3, 2); // 6 rows
    const day_dir = try dayDirPath(a, root);
    defer a.free(day_dir);

    var inputs = try collectDayInputs(a, day_dir);
    defer {
        for (inputs.items) |p| a.free(p);
        inputs.deinit(a);
    }
    std.mem.sort([]u8, inputs.items, {}, lessThanSlice);
    const seq = try seqFromRelPaths(a, inputs.items);
    defer a.free(seq);
    const building = try std.fmt.allocPrint(a, "{s}/{s}{s}", .{ day_dir, seq, building_suffix });
    defer a.free(building);
    try copyMergeToParquet(a, day_dir, inputs.items, building);

    // Committed manifest with the correct output and 2 input lines, one of which
    // is the building basename (ends in `.building`, not `.parquet`) -> the
    // parquet-suffix input check refuses it (and the >=2 input count is met, so
    // it is the suffix check, not the count check, that fires).
    const manifest_path = try manifestPathOwned(a, day_dir, seq);
    defer a.free(manifest_path);
    {
        var body = std.ArrayList(u8){};
        defer body.deinit(a);
        try body.appendSlice(a, seq);
        try body.appendSlice(a, ".parquet\n"); // output line
        try body.appendSlice(a, inputs.items[0]); // a real `.parquet` input
        try body.append(a, '\n');
        try body.appendSlice(a, seq); // and the reserved building artifact
        try body.appendSlice(a, building_suffix);
        try body.append(a, '\n');
        const f = try fs.createFileAbsolute(manifest_path, .{ .mode = 0o600 });
        defer f.close();
        try f.writeAll(body.items);
    }

    try testing.expectError(error.UnsafeManifestPath, recoverCompactions(a, root));
    // The building and all real inputs survive (nothing was deleted): 6 rows.
    try fs.accessAbsolute(building, .{});
    try testing.expectEqual(@as(i64, 6), try dayRowCount(a, root));
}

test "compaction recovery: a truncated manifest with <2 inputs is fatal, inputs kept" {
    // Codex round-5 HIGH: a committed manifest listing fewer than 2 inputs (e.g.
    // just the output line, from a truncating crash) must NOT publish the building
    // while leaving the originals - that double-counts. It is fatal and the day is
    // preserved.
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    try seedSealedDay(a, root, 3, 2); // 6 rows
    const day_dir = try dayDirPath(a, root);
    defer a.free(day_dir);

    var inputs = try collectDayInputs(a, day_dir);
    defer {
        for (inputs.items) |p| a.free(p);
        inputs.deinit(a);
    }
    std.mem.sort([]u8, inputs.items, {}, lessThanSlice);
    const seq = try seqFromRelPaths(a, inputs.items);
    defer a.free(seq);
    const building = try std.fmt.allocPrint(a, "{s}/{s}{s}", .{ day_dir, seq, building_suffix });
    defer a.free(building);
    try copyMergeToParquet(a, day_dir, inputs.items, building);

    // Manifest with ONLY the output line (a truncated commit).
    const manifest_path = try manifestPathOwned(a, day_dir, seq);
    defer a.free(manifest_path);
    {
        const body = try std.fmt.allocPrint(a, "{s}.parquet\n", .{seq});
        defer a.free(body);
        const f = try fs.createFileAbsolute(manifest_path, .{ .mode = 0o600 });
        defer f.close();
        try f.writeAll(body);
    }

    try testing.expectError(error.EmptyManifest, recoverCompactions(a, root));
    // The original inputs are untouched (the building was NOT published over them).
    const after = try countDayFiles(a, root);
    try testing.expectEqual(@as(usize, 3), after.in_hours);
    try testing.expectEqual(@as(usize, 0), after.day_level);
    try testing.expectEqual(@as(i64, 6), try dayRowCount(a, root));
}

test "compaction: isSafeRelPath rejects traversal, separators, controls; accepts real layout" {
    // Codex round-6 MEDIUM: the manifest is newline-delimited delete authority,
    // so a rel path with a newline/CR/control or odd component must be rejected.
    try testing.expect(isSafeRelPath("hour=00/abcd1234.parquet")); // real hour-level
    try testing.expect(isSafeRelPath("abcd1234.parquet")); // real day-level
    try testing.expect(!isSafeRelPath("../escape.parquet"));
    try testing.expect(!isSafeRelPath("/abs.parquet"));
    try testing.expect(!isSafeRelPath("a\nb.parquet")); // embedded newline
    try testing.expect(!isSafeRelPath("a\rb.parquet")); // embedded CR
    try testing.expect(!isSafeRelPath("a\\b.parquet")); // backslash
    try testing.expect(!isSafeRelPath("hour=00//x.parquet")); // empty component
    try testing.expect(!isSafeRelPath("./x.parquet")); // dot component
    try testing.expect(!isSafeRelPath("")); // empty
}

test "compaction: writeManifest refuses an input with a newline (no ambiguous manifest)" {
    // Codex round-6 MEDIUM: writeManifest is the commit authority - it must refuse
    // an input whose name would split into bogus manifest lines, aborting the
    // compaction PRE-COMMIT rather than ever committing a misreadable manifest.
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    const day_dir = try std.fmt.allocPrint(a, "{s}/metrics/date=2023-11-14", .{root});
    defer a.free(day_dir);
    try makePathAbsolute(day_dir);

    var bad = [_][]u8{
        try a.dupe(u8, "hour=00/a.parquet"),
        try a.dupe(u8, "hour=01/b\nc.parquet"), // newline -> ambiguous
    };
    defer for (bad) |b| a.free(b);
    const final = try std.fmt.allocPrint(a, "{s}/deadbeef.parquet", .{day_dir});
    defer a.free(final);

    try testing.expectError(error.UnsafeManifestPath, writeManifest(a, day_dir, "deadbeef", final, &bad));
    // No manifest (or tmp) was committed.
    const mpath = try manifestPathOwned(a, day_dir, "deadbeef");
    defer a.free(mpath);
    try testing.expectError(error.FileNotFound, fs.accessAbsolute(mpath, .{}));
}

test "compaction recovery: a fully-completed day is a clean no-op (idempotent re-run)" {
    // Guards the durability concern: once a compaction is complete (manifest gone,
    // single day-level file), a later recovery pass must NOT touch it - no
    // resurrected manifest, no CompactionOutputMissing, rowcount stable.
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try testRoot(a, &tmp);
    defer a.free(root);

    try seedSealedDay(a, root, 4, 2); // 8 rows
    try compactDay(a, root, .metrics, test_sealed_date);
    try testing.expectEqual(@as(i64, 8), try dayRowCount(a, root));

    // Two more recovery passes: each is a no-op, the day file + rowcount stay put.
    try recoverCompactions(a, root);
    try recoverCompactions(a, root);
    const after = try countDayFiles(a, root);
    try testing.expectEqual(@as(usize, 1), after.day_level);
    try testing.expectEqual(@as(usize, 0), after.in_hours);
    try testing.expectEqual(@as(i64, 8), try dayRowCount(a, root));
}
