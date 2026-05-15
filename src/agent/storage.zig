const std = @import("std");

// C API import
const c = @cImport({
    @cInclude("duckdb.h");
});

// Import types from other modules (named modules via build.zig)
const collector = @import("collector");
const logs = @import("logs");
const proxmox = @import("proxmox");

pub const SystemMetrics = collector.SystemMetrics;
pub const ProcessInfo = collector.ProcessInfo;
pub const DiskInfo = collector.DiskInfo;
pub const LogEntry = logs.LogEntry;
pub const ContainerEntry = proxmox.ContainerEntry;
pub const ContainerMetrics = proxmox.ContainerMetrics;

pub const StorageError = error{
    DatabaseError,
    ConnectionError,
    SchemaError,
    QueryError,
    OutOfMemory,
};

pub const QueryResult = struct {
    columns: [][]const u8,
    rows: [][]?[]const u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *QueryResult) void {
        // Free column names
        for (self.columns) |col| {
            self.allocator.free(col);
        }
        self.allocator.free(self.columns);

        // Free row data
        for (self.rows) |row| {
            for (row) |cell| {
                if (cell) |cell_value| {
                    self.allocator.free(cell_value);
                }
            }
            self.allocator.free(row);
        }
        self.allocator.free(self.rows);
    }
};

pub const Storage = struct {
    db: c.duckdb_database,
    // DuckDB connections are NOT thread-safe. Single-thread-only.
    conn: c.duckdb_connection,
    allocator: std.mem.Allocator,
    // Path the database was opened with, kept so reconnect() can re-init in place.
    db_path: []u8,
    read_only: bool,
    // Counter for consecutive insert failures. The main loop checks this each
    // cycle and calls reconnect() when it crosses reconnect_failure_threshold.
    consecutive_insert_failures: u32 = 0,

    /// After this many consecutive insert failures the daemon should reconnect
    /// to DuckDB once. A handful of transient failures shouldn't reset the
    /// connection; sustained failure means it's wedged.
    pub const reconnect_failure_threshold: u32 = 5;

    pub fn init(allocator: std.mem.Allocator, db_path: []const u8) !Storage {
        return initWithMode(allocator, db_path, false);
    }

    pub fn initReadOnly(allocator: std.mem.Allocator, db_path: []const u8) !Storage {
        return initWithMode(allocator, db_path, true);
    }

    fn initWithMode(allocator: std.mem.Allocator, db_path: []const u8, read_only: bool) !Storage {
        var db: c.duckdb_database = undefined;
        var conn: c.duckdb_connection = undefined;

        // Convert Zig string to null-terminated C string
        const c_path = try allocator.dupeZ(u8, db_path);
        defer allocator.free(c_path);

        // Open database (with optional read-only mode for CLI)
        var open_state: c.duckdb_state = undefined;
        if (read_only) {
            var config: c.duckdb_config = undefined;
            if (c.duckdb_create_config(&config) == c.DuckDBError) {
                return error.DatabaseError;
            }
            defer c.duckdb_destroy_config(&config);
            _ = c.duckdb_set_config(config, "access_mode", "READ_ONLY");
            var err_msg: [*c]u8 = null;
            open_state = c.duckdb_open_ext(c_path.ptr, &db, config, &err_msg);
            if (open_state == c.DuckDBError) {
                if (err_msg) |msg| {
                    std.log.err("DuckDB open error: {s}", .{std.mem.span(msg)});
                    c.duckdb_free(msg);
                }
                return error.DatabaseError;
            }
        } else {
            open_state = c.duckdb_open(c_path.ptr, &db);
            if (open_state == c.DuckDBError) {
                return error.DatabaseError;
            }

            // Restrict the on-disk DuckDB file to 0600 so an unprivileged user
            // on the host can't read collected metrics/logs. Skip for in-memory
            // and non-absolute paths. Best-effort.
            if (!std.mem.eql(u8, db_path, ":memory:") and std.fs.path.isAbsolute(db_path)) {
                if (std.fs.openFileAbsolute(db_path, .{})) |file| {
                    defer file.close();
                    file.chmod(0o600) catch |err| {
                        std.log.warn("DuckDB chmod 0600 failed for {s}: {}", .{ db_path, err });
                    };
                } else |err| {
                    std.log.warn("DuckDB chmod open failed for {s}: {}", .{ db_path, err });
                }
            }
        }

        // Create connection
        const conn_state = c.duckdb_connect(db, &conn);
        if (conn_state == c.DuckDBError) {
            c.duckdb_close(&db);
            return error.ConnectionError;
        }

        // Bound the buffer pool. DuckDB defaults to ~80% of system RAM, which
        // grows unboundedly under the daemon's persistent connection as inserts
        // touch more pages. 128MB is plenty for routine inserts + the hourly
        // retention DELETE; larger working sets spill to a temp file.
        {
            var pragma_result: c.duckdb_result = undefined;
            const pragma_state = c.duckdb_query(conn, "PRAGMA memory_limit='128MB'", &pragma_result);
            defer c.duckdb_destroy_result(&pragma_result);
            if (pragma_state == c.DuckDBError) {
                const err_msg = c.duckdb_result_error(&pragma_result);
                std.log.err("DuckDB memory_limit pragma failed: {s}", .{std.mem.span(err_msg)});
                c.duckdb_disconnect(&conn);
                c.duckdb_close(&db);
                return error.DatabaseError;
            }
        }

        const db_path_owned = try allocator.dupe(u8, db_path);
        errdefer allocator.free(db_path_owned);

        var storage = Storage{
            .db = db,
            .conn = conn,
            .allocator = allocator,
            .db_path = db_path_owned,
            .read_only = read_only,
        };

        // Initialize schema (skip for read-only mode)
        if (!read_only) {
            try storage.initSchema();
        }

        return storage;
    }

    pub fn deinit(self: *Storage) void {
        c.duckdb_disconnect(&self.conn);
        c.duckdb_close(&self.db);
        self.allocator.free(self.db_path);
    }

    /// On-disk size of the main DuckDB file in bytes. Reads `stat` on the
    /// `db_path`; returns 0 if the path can't be statted (in-memory DBs,
    /// transient FS errors). The WAL and tmp/ sidecar are intentionally
    /// excluded - the checkpoint-OOM failure mode is driven by the main
    /// file's size, which is what alerting cares about.
    pub fn dbSizeBytes(self: *const Storage) u64 {
        if (std.mem.eql(u8, self.db_path, ":memory:")) return 0;
        const stat = std.fs.cwd().statFile(self.db_path) catch return 0;
        return stat.size;
    }

    /// Tear down the existing duckdb connection + database handle and re-open
    /// them from `self.db_path`. Called by the main loop when consecutive
    /// insert failures cross `reconnect_failure_threshold`. The caller should
    /// reset `consecutive_insert_failures` after calling this so a single
    /// re-init burst doesn't immediately re-trigger.
    pub fn reconnect(self: *Storage) !void {
        const c_path = try self.allocator.dupeZ(u8, self.db_path);
        defer self.allocator.free(c_path);

        var new_db: c.duckdb_database = undefined;
        var new_conn: c.duckdb_connection = undefined;

        const open_state = c.duckdb_open(c_path.ptr, &new_db);
        if (open_state == c.DuckDBError) {
            return error.DatabaseError;
        }
        errdefer c.duckdb_close(&new_db);

        const conn_state = c.duckdb_connect(new_db, &new_conn);
        if (conn_state == c.DuckDBError) {
            return error.ConnectionError;
        }
        errdefer c.duckdb_disconnect(&new_conn);

        // Re-apply the buffer-pool bound (matches initWithMode).
        var pragma_result: c.duckdb_result = undefined;
        const pragma_state = c.duckdb_query(new_conn, "PRAGMA memory_limit='128MB'", &pragma_result);
        defer c.duckdb_destroy_result(&pragma_result);
        if (pragma_state == c.DuckDBError) {
            const err_msg = c.duckdb_result_error(&pragma_result);
            std.log.err("DuckDB memory_limit pragma failed on reconnect: {s}", .{std.mem.span(err_msg)});
            return error.DatabaseError;
        }

        var replacement = Storage{
            .db = new_db,
            .conn = new_conn,
            .allocator = self.allocator,
            .db_path = self.db_path,
            .read_only = self.read_only,
            .consecutive_insert_failures = self.consecutive_insert_failures,
        };
        if (!replacement.read_only) {
            try replacement.initSchema();
        }

        var old_conn = self.conn;
        var old_db = self.db;
        self.db = new_db;
        self.conn = new_conn;
        c.duckdb_disconnect(&old_conn);
        c.duckdb_close(&old_db);
    }

    fn initSchema(self: *Storage) !void {
        const schema_sql =
            \\CREATE TABLE IF NOT EXISTS metrics (
            \\  timestamp TIMESTAMP NOT NULL,
            \\  cpu_percent REAL,
            \\  cpu_user REAL,
            \\  cpu_system REAL,
            \\  cpu_iowait REAL,
            \\  mem_total BIGINT,
            \\  mem_used BIGINT,
            \\  mem_percent REAL,
            \\  swap_total BIGINT,
            \\  swap_used BIGINT
            \\);
            \\CREATE INDEX IF NOT EXISTS idx_metrics_ts ON metrics(timestamp);
            \\
            \\CREATE TABLE IF NOT EXISTS processes (
            \\  timestamp TIMESTAMP NOT NULL,
            \\  pid INTEGER,
            \\  name VARCHAR,
            \\  cmdline VARCHAR,
            \\  state CHAR(1),
            \\  cpu_percent REAL,
            \\  mem_rss BIGINT,
            \\  threads INTEGER,
            \\  username VARCHAR
            \\);
            \\CREATE INDEX IF NOT EXISTS idx_processes_ts ON processes(timestamp);
            \\CREATE INDEX IF NOT EXISTS idx_processes_name ON processes(name);
            \\
            \\CREATE TABLE IF NOT EXISTS disks (
            \\  timestamp TIMESTAMP NOT NULL,
            \\  mount_point VARCHAR,
            \\  filesystem VARCHAR,
            \\  total_bytes BIGINT,
            \\  used_bytes BIGINT,
            \\  percent REAL
            \\);
            \\
            \\CREATE TABLE IF NOT EXISTS logs (
            \\  timestamp TIMESTAMP NOT NULL,
            \\  source VARCHAR,
            \\  unit VARCHAR,
            \\  identifier VARCHAR,
            \\  systemd_unit VARCHAR,
            \\  priority INTEGER,
            \\  message TEXT,
            \\  pid INTEGER
            \\);
            \\ALTER TABLE logs ADD COLUMN IF NOT EXISTS identifier VARCHAR;
            \\ALTER TABLE logs ADD COLUMN IF NOT EXISTS systemd_unit VARCHAR;
            \\CREATE INDEX IF NOT EXISTS idx_logs_ts ON logs(timestamp);
            \\CREATE INDEX IF NOT EXISTS idx_logs_unit ON logs(unit);
            \\CREATE INDEX IF NOT EXISTS idx_logs_identifier ON logs(identifier);
            \\CREATE INDEX IF NOT EXISTS idx_logs_systemd_unit ON logs(systemd_unit);
            \\CREATE INDEX IF NOT EXISTS idx_logs_priority ON logs(priority);
            \\
            \\CREATE TABLE IF NOT EXISTS containers (
            \\  timestamp TIMESTAMP NOT NULL,
            \\  vmid INTEGER NOT NULL,
            \\  name VARCHAR,
            \\  node VARCHAR,
            \\  type VARCHAR,
            \\  status VARCHAR,
            \\  maxmem BIGINT,
            \\  maxcpu DOUBLE,
            \\  uptime BIGINT
            \\);
            \\CREATE INDEX IF NOT EXISTS idx_containers_ts ON containers(timestamp);
            \\CREATE INDEX IF NOT EXISTS idx_containers_vmid ON containers(vmid);
            \\
            \\CREATE TABLE IF NOT EXISTS container_metrics (
            \\  timestamp TIMESTAMP NOT NULL,
            \\  vmid INTEGER NOT NULL,
            \\  cpu_pct DOUBLE,
            \\  mem_current BIGINT,
            \\  mem_max BIGINT
            \\);
            \\CREATE INDEX IF NOT EXISTS idx_container_metrics_ts ON container_metrics(timestamp);
            \\CREATE INDEX IF NOT EXISTS idx_container_metrics_vmid ON container_metrics(vmid);
        ;

        var result: c.duckdb_result = undefined;
        const state = c.duckdb_query(self.conn, schema_sql.ptr, &result);
        defer c.duckdb_destroy_result(&result);

        if (state == c.DuckDBError) {
            const err_msg = c.duckdb_result_error(&result);
            std.log.err("Schema creation error: {s}", .{err_msg});
            return error.SchemaError;
        }
    }

    pub fn insertMetrics(self: *Storage, timestamp: i64, metrics: SystemMetrics) !void {
        errdefer self.consecutive_insert_failures +|= 1;

        const sql = "INSERT INTO metrics VALUES (to_timestamp($1), $2, $3, $4, $5, $6, $7, $8, $9, $10)";

        var stmt: c.duckdb_prepared_statement = undefined;
        var state = c.duckdb_prepare(self.conn, sql.ptr, &stmt);
        if (state == c.DuckDBError) {
            return error.QueryError;
        }
        defer c.duckdb_destroy_prepare(&stmt);

        _ = c.duckdb_bind_int64(stmt, 1, timestamp);
        _ = c.duckdb_bind_float(stmt, 2, metrics.cpu_percent);
        _ = c.duckdb_bind_float(stmt, 3, metrics.cpu_user);
        _ = c.duckdb_bind_float(stmt, 4, metrics.cpu_system);
        _ = c.duckdb_bind_float(stmt, 5, metrics.cpu_iowait);
        _ = c.duckdb_bind_uint64(stmt, 6, metrics.mem_total);
        _ = c.duckdb_bind_uint64(stmt, 7, metrics.mem_used);
        _ = c.duckdb_bind_float(stmt, 8, metrics.mem_percent);
        _ = c.duckdb_bind_uint64(stmt, 9, metrics.swap_total);
        _ = c.duckdb_bind_uint64(stmt, 10, metrics.swap_used);

        var result: c.duckdb_result = undefined;
        state = c.duckdb_execute_prepared(stmt, &result);
        defer c.duckdb_destroy_result(&result);

        if (state == c.DuckDBError) {
            const err_msg = c.duckdb_result_error(&result);
            std.log.err("Insert metrics error: {s}", .{err_msg});
            return error.QueryError;
        }

        self.consecutive_insert_failures = 0;
    }

    pub fn insertProcesses(self: *Storage, timestamp: i64, procs: []const ProcessInfo) !void {
        errdefer self.consecutive_insert_failures +|= 1;

        var appender: c.duckdb_appender = undefined;
        var state = c.duckdb_appender_create(self.conn, null, "processes", &appender);
        if (state == c.DuckDBError) {
            return error.QueryError;
        }
        defer _ = c.duckdb_appender_destroy(&appender);

        for (procs) |proc| {
            _ = c.duckdb_append_timestamp(appender, .{ .micros = timestamp * 1000000 });
            _ = c.duckdb_append_uint32(appender, proc.pid);
            _ = c.duckdb_append_varchar_length(appender, proc.name.ptr, proc.name.len);
            _ = c.duckdb_append_varchar_length(appender, proc.cmdline.ptr, proc.cmdline.len);
            const state_str = [_]u8{proc.state};
            _ = c.duckdb_append_varchar_length(appender, &state_str, 1);
            _ = c.duckdb_append_float(appender, proc.cpu_percent);
            _ = c.duckdb_append_uint64(appender, proc.mem_rss);
            _ = c.duckdb_append_uint32(appender, proc.threads);
            _ = c.duckdb_append_varchar_length(appender, proc.username.ptr, proc.username.len);

            state = c.duckdb_appender_end_row(appender);
            if (state == c.DuckDBError) {
                const err = c.duckdb_appender_error(appender);
                std.log.err("Append process row error: {s}", .{err});
                return error.QueryError;
            }
        }

        state = c.duckdb_appender_flush(appender);
        if (state == c.DuckDBError) {
            const err = c.duckdb_appender_error(appender);
            std.log.err("Flush processes error: {s}", .{err});
            return error.QueryError;
        }

        self.consecutive_insert_failures = 0;
    }

    pub fn insertDisks(self: *Storage, timestamp: i64, disks: []const DiskInfo) !void {
        errdefer self.consecutive_insert_failures +|= 1;

        var appender: c.duckdb_appender = undefined;
        var state = c.duckdb_appender_create(self.conn, null, "disks", &appender);
        if (state == c.DuckDBError) {
            return error.QueryError;
        }
        defer _ = c.duckdb_appender_destroy(&appender);

        for (disks) |disk| {
            _ = c.duckdb_append_timestamp(appender, .{ .micros = timestamp * 1000000 });
            _ = c.duckdb_append_varchar_length(appender, disk.mount_point.ptr, disk.mount_point.len);
            _ = c.duckdb_append_varchar_length(appender, disk.filesystem.ptr, disk.filesystem.len);
            _ = c.duckdb_append_uint64(appender, disk.total_bytes);
            _ = c.duckdb_append_uint64(appender, disk.used_bytes);
            _ = c.duckdb_append_float(appender, disk.percent);

            state = c.duckdb_appender_end_row(appender);
            if (state == c.DuckDBError) {
                const err = c.duckdb_appender_error(appender);
                std.log.err("Append disk row error: {s}", .{err});
                return error.QueryError;
            }
        }

        state = c.duckdb_appender_flush(appender);
        if (state == c.DuckDBError) {
            const err = c.duckdb_appender_error(appender);
            std.log.err("Flush disks error: {s}", .{err});
            return error.QueryError;
        }

        self.consecutive_insert_failures = 0;
    }

    pub fn insertContainers(self: *Storage, timestamp: i64, containers: []const ContainerEntry) !void {
        errdefer self.consecutive_insert_failures +|= 1;

        var appender: c.duckdb_appender = undefined;
        var state = c.duckdb_appender_create(self.conn, null, "containers", &appender);
        if (state == c.DuckDBError) {
            return error.QueryError;
        }
        defer _ = c.duckdb_appender_destroy(&appender);

        for (containers) |entry| {
            _ = c.duckdb_append_timestamp(appender, .{ .micros = timestamp * 1000000 });
            _ = c.duckdb_append_uint32(appender, entry.vmid);
            _ = c.duckdb_append_varchar_length(appender, entry.name.ptr, entry.name.len);
            _ = c.duckdb_append_varchar_length(appender, entry.node.ptr, entry.node.len);
            _ = c.duckdb_append_varchar_length(appender, entry.type.ptr, entry.type.len);
            _ = c.duckdb_append_varchar_length(appender, entry.status.ptr, entry.status.len);
            _ = c.duckdb_append_uint64(appender, entry.maxmem);
            _ = c.duckdb_append_double(appender, entry.maxcpu);
            _ = c.duckdb_append_uint64(appender, entry.uptime);

            state = c.duckdb_appender_end_row(appender);
            if (state == c.DuckDBError) {
                const err = c.duckdb_appender_error(appender);
                std.log.err("Append container row error: {s}", .{err});
                return error.QueryError;
            }
        }

        state = c.duckdb_appender_flush(appender);
        if (state == c.DuckDBError) {
            const err = c.duckdb_appender_error(appender);
            std.log.err("Flush containers error: {s}", .{err});
            return error.QueryError;
        }

        self.consecutive_insert_failures = 0;
    }

    pub fn insertContainerMetrics(self: *Storage, timestamp: i64, samples: []const ContainerMetrics) !void {
        errdefer self.consecutive_insert_failures +|= 1;

        var appender: c.duckdb_appender = undefined;
        var state = c.duckdb_appender_create(self.conn, null, "container_metrics", &appender);
        if (state == c.DuckDBError) {
            return error.QueryError;
        }
        defer _ = c.duckdb_appender_destroy(&appender);

        for (samples) |sample| {
            _ = c.duckdb_append_timestamp(appender, .{ .micros = timestamp * 1000000 });
            _ = c.duckdb_append_uint32(appender, sample.vmid);
            // NaN sentinel = first cycle for this CT, no delta yet. Persist
            // as NULL so the dashboard doesn't render a misleading 0% spike
            // when a CT first appears.
            if (std.math.isNan(sample.cpu_pct)) {
                _ = c.duckdb_append_null(appender);
            } else {
                _ = c.duckdb_append_double(appender, sample.cpu_pct);
            }
            _ = c.duckdb_append_uint64(appender, sample.mem_current);
            if (sample.mem_max) |m| {
                _ = c.duckdb_append_uint64(appender, m);
            } else {
                _ = c.duckdb_append_null(appender);
            }

            state = c.duckdb_appender_end_row(appender);
            if (state == c.DuckDBError) {
                const err = c.duckdb_appender_error(appender);
                std.log.err("Append container_metric row error: {s}", .{err});
                return error.QueryError;
            }
        }

        state = c.duckdb_appender_flush(appender);
        if (state == c.DuckDBError) {
            const err = c.duckdb_appender_error(appender);
            std.log.err("Flush container_metrics error: {s}", .{err});
            return error.QueryError;
        }

        self.consecutive_insert_failures = 0;
    }

    pub fn insertLog(self: *Storage, entry: LogEntry) !void {
        errdefer self.consecutive_insert_failures +|= 1;

        const sql = "INSERT INTO logs (timestamp, source, unit, identifier, systemd_unit, priority, message, pid) VALUES (to_timestamp($1), $2, $3, $4, $5, $6, $7, $8)";

        var stmt: c.duckdb_prepared_statement = undefined;
        var state = c.duckdb_prepare(self.conn, sql.ptr, &stmt);
        if (state == c.DuckDBError) {
            return error.QueryError;
        }
        defer c.duckdb_destroy_prepare(&stmt);

        _ = c.duckdb_bind_int64(stmt, 1, entry.timestamp);
        _ = c.duckdb_bind_varchar_length(stmt, 2, entry.source.ptr, entry.source.len);

        if (entry.unit) |unit| {
            _ = c.duckdb_bind_varchar_length(stmt, 3, unit.ptr, unit.len);
        } else {
            _ = c.duckdb_bind_null(stmt, 3);
        }

        if (entry.identifier) |identifier| {
            _ = c.duckdb_bind_varchar_length(stmt, 4, identifier.ptr, identifier.len);
        } else {
            _ = c.duckdb_bind_null(stmt, 4);
        }

        if (entry.systemd_unit) |systemd_unit| {
            _ = c.duckdb_bind_varchar_length(stmt, 5, systemd_unit.ptr, systemd_unit.len);
        } else {
            _ = c.duckdb_bind_null(stmt, 5);
        }

        _ = c.duckdb_bind_uint8(stmt, 6, entry.priority);
        _ = c.duckdb_bind_varchar_length(stmt, 7, entry.message.ptr, entry.message.len);

        if (entry.pid) |pid| {
            _ = c.duckdb_bind_uint32(stmt, 8, pid);
        } else {
            _ = c.duckdb_bind_null(stmt, 8);
        }

        var result: c.duckdb_result = undefined;
        state = c.duckdb_execute_prepared(stmt, &result);
        defer c.duckdb_destroy_result(&result);

        if (state == c.DuckDBError) {
            const err_msg = c.duckdb_result_error(&result);
            std.log.err("Insert log error: {s}", .{err_msg});
            return error.QueryError;
        }

        self.consecutive_insert_failures = 0;
    }

    pub fn getLatestMetrics(self: *Storage) !?SystemMetrics {
        const sql = "SELECT * FROM metrics ORDER BY timestamp DESC LIMIT 1";

        var result: c.duckdb_result = undefined;
        const state = c.duckdb_query(self.conn, sql.ptr, &result);
        defer c.duckdb_destroy_result(&result);

        if (state == c.DuckDBError) {
            const err_msg = c.duckdb_result_error(&result);
            std.log.err("Query latest metrics error: {s}", .{err_msg});
            return error.QueryError;
        }

        const row_count = c.duckdb_row_count(&result);
        if (row_count == 0) {
            return null;
        }

        return SystemMetrics{
            .cpu_percent = c.duckdb_value_float(&result, 1, 0),
            .cpu_user = c.duckdb_value_float(&result, 2, 0),
            .cpu_system = c.duckdb_value_float(&result, 3, 0),
            .cpu_iowait = c.duckdb_value_float(&result, 4, 0),
            .mem_total = c.duckdb_value_uint64(&result, 5, 0),
            .mem_used = c.duckdb_value_uint64(&result, 6, 0),
            .mem_percent = c.duckdb_value_float(&result, 7, 0),
            .swap_total = c.duckdb_value_uint64(&result, 8, 0),
            .swap_used = c.duckdb_value_uint64(&result, 9, 0),
        };
    }

    pub fn getMetricsRange(self: *Storage, since: i64, until: i64) ![]SystemMetrics {
        const sql = "SELECT * FROM metrics WHERE timestamp >= to_timestamp($1) AND timestamp <= to_timestamp($2) ORDER BY timestamp";

        var stmt: c.duckdb_prepared_statement = undefined;
        var state = c.duckdb_prepare(self.conn, sql.ptr, &stmt);
        if (state == c.DuckDBError) {
            return error.QueryError;
        }
        defer c.duckdb_destroy_prepare(&stmt);

        _ = c.duckdb_bind_int64(stmt, 1, since);
        _ = c.duckdb_bind_int64(stmt, 2, until);

        var result: c.duckdb_result = undefined;
        state = c.duckdb_execute_prepared(stmt, &result);
        defer c.duckdb_destroy_result(&result);

        if (state == c.DuckDBError) {
            const err_msg = c.duckdb_result_error(&result);
            std.log.err("Query metrics range error: {s}", .{err_msg});
            return error.QueryError;
        }

        const row_count = c.duckdb_row_count(&result);
        if (row_count == 0) {
            return &[_]SystemMetrics{};
        }

        const metrics = try self.allocator.alloc(SystemMetrics, row_count);
        errdefer self.allocator.free(metrics);

        var i: usize = 0;
        while (i < row_count) : (i += 1) {
            metrics[i] = SystemMetrics{
                .cpu_percent = c.duckdb_value_float(&result, 1, i),
                .cpu_user = c.duckdb_value_float(&result, 2, i),
                .cpu_system = c.duckdb_value_float(&result, 3, i),
                .cpu_iowait = c.duckdb_value_float(&result, 4, i),
                .mem_total = c.duckdb_value_uint64(&result, 5, i),
                .mem_used = c.duckdb_value_uint64(&result, 6, i),
                .mem_percent = c.duckdb_value_float(&result, 7, i),
                .swap_total = c.duckdb_value_uint64(&result, 8, i),
                .swap_used = c.duckdb_value_uint64(&result, 9, i),
            };
        }

        return metrics;
    }

    pub fn getProcesses(self: *Storage, timestamp: ?i64) ![]ProcessInfo {
        const sql = if (timestamp) |_|
            "SELECT * FROM processes WHERE timestamp = $1 ORDER BY cpu_percent DESC"
        else
            "SELECT * FROM processes WHERE timestamp = (SELECT MAX(timestamp) FROM processes) ORDER BY cpu_percent DESC";

        var result: c.duckdb_result = undefined;
        const state = if (timestamp) |ts| blk: {
            var stmt: c.duckdb_prepared_statement = undefined;
            const prep_state = c.duckdb_prepare(self.conn, sql.ptr, &stmt);
            if (prep_state == c.DuckDBError) {
                return error.QueryError;
            }
            defer c.duckdb_destroy_prepare(&stmt);

            _ = c.duckdb_bind_timestamp(stmt, 1, .{ .micros = ts * 1000000 });
            break :blk c.duckdb_execute_prepared(stmt, &result);
        } else c.duckdb_query(self.conn, sql.ptr, &result);

        defer c.duckdb_destroy_result(&result);

        if (state == c.DuckDBError) {
            const err_msg = c.duckdb_result_error(&result);
            std.log.err("Query processes error: {s}", .{err_msg});
            return error.QueryError;
        }

        const row_count = c.duckdb_row_count(&result);
        if (row_count == 0) {
            return &[_]ProcessInfo{};
        }

        const procs = try self.allocator.alloc(ProcessInfo, row_count);
        errdefer self.allocator.free(procs);

        var i: usize = 0;
        while (i < row_count) : (i += 1) {
            const pid = c.duckdb_value_uint32(&result, 1, i);
            const name_ptr = c.duckdb_value_varchar(&result, 2, i);
            const name = try self.allocator.dupe(u8, std.mem.span(name_ptr));
            errdefer self.allocator.free(name);

            const cmdline_ptr = c.duckdb_value_varchar(&result, 3, i);
            const cmdline = try self.allocator.dupe(u8, std.mem.span(cmdline_ptr));
            errdefer self.allocator.free(cmdline);

            const state_ptr = c.duckdb_value_varchar(&result, 4, i);
            const state_char = std.mem.span(state_ptr)[0];

            const cpu_percent = c.duckdb_value_float(&result, 5, i);
            const mem_rss = c.duckdb_value_uint64(&result, 6, i);
            const threads = c.duckdb_value_uint32(&result, 7, i);

            const username_ptr = c.duckdb_value_varchar(&result, 8, i);
            const username = try self.allocator.dupe(u8, std.mem.span(username_ptr));
            errdefer self.allocator.free(username);

            procs[i] = ProcessInfo{
                .pid = pid,
                .name = name,
                .cmdline = cmdline,
                .state = state_char,
                .cpu_percent = cpu_percent,
                .mem_rss = mem_rss,
                .threads = threads,
                .username = username,
            };
        }

        return procs;
    }

    pub fn queryLogs(self: *Storage, since: ?i64, unit: ?[]const u8, priority: ?u8) ![]LogEntry {
        const sql = "SELECT timestamp, source, unit, identifier, systemd_unit, priority, message, pid FROM logs WHERE ($1::BIGINT IS NULL OR timestamp >= to_timestamp($1)) AND ($2::VARCHAR IS NULL OR unit = $2) AND ($3::INTEGER IS NULL OR priority = $3) ORDER BY timestamp DESC";

        var stmt: c.duckdb_prepared_statement = undefined;
        var state = c.duckdb_prepare(self.conn, sql.ptr, &stmt);
        if (state == c.DuckDBError) {
            return error.QueryError;
        }
        defer c.duckdb_destroy_prepare(&stmt);

        if (since) |s| {
            _ = c.duckdb_bind_int64(stmt, 1, s);
        } else {
            _ = c.duckdb_bind_null(stmt, 1);
        }

        if (unit) |u| {
            _ = c.duckdb_bind_varchar_length(stmt, 2, u.ptr, u.len);
        } else {
            _ = c.duckdb_bind_null(stmt, 2);
        }

        if (priority) |p| {
            _ = c.duckdb_bind_uint8(stmt, 3, p);
        } else {
            _ = c.duckdb_bind_null(stmt, 3);
        }

        var result: c.duckdb_result = undefined;
        state = c.duckdb_execute_prepared(stmt, &result);
        defer c.duckdb_destroy_result(&result);

        if (state == c.DuckDBError) {
            const err_msg = c.duckdb_result_error(&result);
            std.log.err("Query logs error: {s}", .{err_msg});
            return error.QueryError;
        }

        const row_count = c.duckdb_row_count(&result);
        if (row_count == 0) {
            return &[_]LogEntry{};
        }

        const entries = try self.allocator.alloc(LogEntry, row_count);
        errdefer self.allocator.free(entries);

        var i: usize = 0;
        while (i < row_count) : (i += 1) {
            const ts_struct = c.duckdb_value_timestamp(&result, 0, i);
            const timestamp = @divTrunc(ts_struct.micros, 1000000);

            const source_ptr = c.duckdb_value_varchar(&result, 1, i);
            const source = try self.allocator.dupe(u8, std.mem.span(source_ptr));
            errdefer self.allocator.free(source);

            const unit_val = if (c.duckdb_value_is_null(&result, 2, i)) null else blk: {
                const unit_ptr = c.duckdb_value_varchar(&result, 2, i);
                break :blk try self.allocator.dupe(u8, std.mem.span(unit_ptr));
            };
            errdefer if (unit_val) |u| self.allocator.free(u);

            const identifier_val = if (c.duckdb_value_is_null(&result, 3, i)) null else blk: {
                const identifier_ptr = c.duckdb_value_varchar(&result, 3, i);
                break :blk try self.allocator.dupe(u8, std.mem.span(identifier_ptr));
            };
            errdefer if (identifier_val) |identifier| self.allocator.free(identifier);

            const systemd_unit_val = if (c.duckdb_value_is_null(&result, 4, i)) null else blk: {
                const systemd_unit_ptr = c.duckdb_value_varchar(&result, 4, i);
                break :blk try self.allocator.dupe(u8, std.mem.span(systemd_unit_ptr));
            };
            errdefer if (systemd_unit_val) |systemd_unit| self.allocator.free(systemd_unit);

            const prio = c.duckdb_value_uint8(&result, 5, i);

            const message_ptr = c.duckdb_value_varchar(&result, 6, i);
            const message = try self.allocator.dupe(u8, std.mem.span(message_ptr));
            errdefer self.allocator.free(message);

            const pid_val = if (c.duckdb_value_is_null(&result, 7, i)) null else c.duckdb_value_uint32(&result, 7, i);

            entries[i] = LogEntry{
                .timestamp = timestamp,
                .source = source,
                .unit = unit_val,
                .identifier = identifier_val,
                .systemd_unit = systemd_unit_val,
                .priority = prio,
                .message = message,
                .pid = pid_val,
            };
        }

        return entries;
    }

    pub fn rawQuery(self: *Storage, sql: []const u8) !QueryResult {
        const c_sql = try self.allocator.dupeZ(u8, sql);
        defer self.allocator.free(c_sql);

        var result: c.duckdb_result = undefined;
        const state = c.duckdb_query(self.conn, c_sql.ptr, &result);
        defer c.duckdb_destroy_result(&result);

        if (state == c.DuckDBError) {
            const err_msg = c.duckdb_result_error(&result);
            std.log.err("Raw query error: {s}", .{err_msg});
            return error.QueryError;
        }

        const col_count = c.duckdb_column_count(&result);
        const row_count = c.duckdb_row_count(&result);

        // Allocate column names
        const columns = try self.allocator.alloc([]const u8, col_count);
        errdefer self.allocator.free(columns);

        var col_idx: usize = 0;
        while (col_idx < col_count) : (col_idx += 1) {
            const col_name = c.duckdb_column_name(&result, col_idx);
            columns[col_idx] = try self.allocator.dupe(u8, std.mem.span(col_name));
        }

        // Allocate rows
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
                    const val_ptr = c.duckdb_value_varchar(&result, col_idx, row_idx);
                    rows[row_idx][col_idx] = try self.allocator.dupe(u8, std.mem.span(val_ptr));
                }
            }
        }

        return QueryResult{
            .columns = columns,
            .rows = rows,
            .allocator = self.allocator,
        };
    }

    pub fn getDisks(self: *Storage, timestamp: ?i64) ![]DiskInfo {
        const sql = if (timestamp) |_|
            "SELECT * FROM disks WHERE timestamp = $1 ORDER BY mount_point"
        else
            "SELECT * FROM disks WHERE timestamp = (SELECT MAX(timestamp) FROM disks) ORDER BY mount_point";

        var result: c.duckdb_result = undefined;
        const state = if (timestamp) |ts| blk: {
            var stmt: c.duckdb_prepared_statement = undefined;
            const prep_state = c.duckdb_prepare(self.conn, sql.ptr, &stmt);
            if (prep_state == c.DuckDBError) {
                return error.QueryError;
            }
            defer c.duckdb_destroy_prepare(&stmt);

            _ = c.duckdb_bind_timestamp(stmt, 1, .{ .micros = ts * 1000000 });
            break :blk c.duckdb_execute_prepared(stmt, &result);
        } else c.duckdb_query(self.conn, sql.ptr, &result);

        defer c.duckdb_destroy_result(&result);

        if (state == c.DuckDBError) {
            return error.QueryError;
        }

        const row_count = c.duckdb_row_count(&result);
        if (row_count == 0) {
            return &[_]DiskInfo{};
        }

        const disks = try self.allocator.alloc(DiskInfo, row_count);
        errdefer self.allocator.free(disks);

        var i: usize = 0;
        while (i < row_count) : (i += 1) {
            const mount_ptr = c.duckdb_value_varchar(&result, 1, i);
            const mount_point = try self.allocator.dupe(u8, std.mem.span(mount_ptr));
            errdefer self.allocator.free(mount_point);

            const fs_ptr = c.duckdb_value_varchar(&result, 2, i);
            const filesystem = try self.allocator.dupe(u8, std.mem.span(fs_ptr));
            errdefer self.allocator.free(filesystem);

            disks[i] = DiskInfo{
                .mount_point = mount_point,
                .filesystem = filesystem,
                .total_bytes = c.duckdb_value_uint64(&result, 3, i),
                .used_bytes = c.duckdb_value_uint64(&result, 4, i),
                .percent = c.duckdb_value_float(&result, 5, i),
            };
        }

        return disks;
    }

    pub fn runRetention(self: *Storage, max_age_seconds: i64) !void {
        const tables = [_][]const u8{ "metrics", "processes", "disks", "logs", "containers", "container_metrics" };

        for (tables) |table| {
            const sql = try std.fmt.allocPrint(
                self.allocator,
                "DELETE FROM {s} WHERE timestamp < CURRENT_TIMESTAMP - INTERVAL '{d} seconds'",
                .{ table, max_age_seconds },
            );
            defer self.allocator.free(sql);

            const c_sql = try self.allocator.dupeZ(u8, sql);
            defer self.allocator.free(c_sql);

            var result: c.duckdb_result = undefined;
            const state = c.duckdb_query(self.conn, c_sql.ptr, &result);
            defer c.duckdb_destroy_result(&result);

            if (state == c.DuckDBError) {
                const err_msg = c.duckdb_result_error(&result);
                std.log.err("Retention cleanup error for {s}: {s}", .{ table, err_msg });
                return error.QueryError;
            }
        }
    }
};

test "Storage: init and schema creation" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, ":memory:");
    defer storage.deinit();

    // Verify tables exist by querying them
    const tables = [_][]const u8{ "metrics", "processes", "disks", "logs", "containers", "container_metrics" };
    for (tables) |table| {
        const sql = try std.fmt.allocPrint(allocator, "SELECT COUNT(*) FROM {s}", .{table});
        defer allocator.free(sql);

        const c_sql = try allocator.dupeZ(u8, sql);
        defer allocator.free(c_sql);

        var result: c.duckdb_result = undefined;
        const state = c.duckdb_query(storage.conn, c_sql.ptr, &result);
        defer c.duckdb_destroy_result(&result);

        try std.testing.expect(state == c.DuckDBSuccess);
    }
}

test "Storage: failed reconnect leaves existing handles usable" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, ":memory:");
    defer storage.deinit();

    const original_path = storage.db_path;
    const bad_path = try allocator.dupe(u8, "/nonexistent/sermon-daemon-test/db.duckdb");
    storage.db_path = bad_path;
    defer {
        allocator.free(storage.db_path);
        storage.db_path = original_path;
    }

    try std.testing.expectError(error.DatabaseError, storage.reconnect());

    const metrics = SystemMetrics{
        .cpu_percent = 45.5,
        .cpu_user = 30.2,
        .cpu_system = 15.3,
        .cpu_iowait = 2.5,
        .mem_total = 16000000000,
        .mem_used = 8000000000,
        .mem_percent = 50.0,
        .swap_total = 4000000000,
        .swap_used = 1000000000,
    };
    try storage.insertMetrics(std.time.timestamp(), metrics);
}

test "Storage: reconnect initializes schema on fresh database" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, ":memory:");
    defer storage.deinit();

    try storage.reconnect();

    const metrics = SystemMetrics{
        .cpu_percent = 45.5,
        .cpu_user = 30.2,
        .cpu_system = 15.3,
        .cpu_iowait = 2.5,
        .mem_total = 16000000000,
        .mem_used = 8000000000,
        .mem_percent = 50.0,
        .swap_total = 4000000000,
        .swap_used = 1000000000,
    };
    try storage.insertMetrics(std.time.timestamp(), metrics);
}

test "Storage: insert and retrieve metrics" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, ":memory:");
    defer storage.deinit();

    const metrics = SystemMetrics{
        .cpu_percent = 45.5,
        .cpu_user = 30.2,
        .cpu_system = 15.3,
        .cpu_iowait = 2.5,
        .mem_total = 16000000000,
        .mem_used = 8000000000,
        .mem_percent = 50.0,
        .swap_total = 4000000000,
        .swap_used = 1000000000,
    };

    const timestamp = std.time.timestamp();
    try storage.insertMetrics(timestamp, metrics);

    const retrieved = try storage.getLatestMetrics();
    try std.testing.expect(retrieved != null);

    const m = retrieved.?;
    try std.testing.expectApproxEqAbs(metrics.cpu_percent, m.cpu_percent, 0.1);
    try std.testing.expectApproxEqAbs(metrics.cpu_user, m.cpu_user, 0.1);
    try std.testing.expect(m.mem_total == metrics.mem_total);
    try std.testing.expect(m.mem_used == metrics.mem_used);
}

test "Storage: insert and retrieve processes" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, ":memory:");
    defer storage.deinit();

    const proc1 = ProcessInfo{
        .pid = 1234,
        .name = "test_proc",
        .cmdline = "/usr/bin/test_proc --flag",
        .state = 'R',
        .cpu_percent = 12.5,
        .mem_rss = 50000000,
        .threads = 4,
        .username = "testuser",
    };

    const procs = [_]ProcessInfo{proc1};
    const timestamp = std.time.timestamp();
    try storage.insertProcesses(timestamp, &procs);

    const retrieved = try storage.getProcesses(timestamp);
    defer {
        for (retrieved) |p| {
            allocator.free(p.name);
            allocator.free(p.cmdline);
            allocator.free(p.username);
        }
        allocator.free(retrieved);
    }

    try std.testing.expect(retrieved.len == 1);
    try std.testing.expect(retrieved[0].pid == proc1.pid);
    try std.testing.expectEqualStrings(proc1.name, retrieved[0].name);
    try std.testing.expect(retrieved[0].state == 'R');
}

test "Storage: insert and query logs" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, ":memory:");
    defer storage.deinit();

    const entry = LogEntry{
        .timestamp = std.time.timestamp(),
        .source = "test_source",
        .unit = "test",
        .identifier = "test",
        .systemd_unit = "test.service",
        .priority = 6,
        .message = "Test log message",
        .pid = 5678,
    };

    try storage.insertLog(entry);

    const log_entries = try storage.queryLogs(null, null, null);
    defer {
        for (log_entries) |log| {
            allocator.free(log.source);
            if (log.unit) |u| allocator.free(u);
            if (log.identifier) |identifier| allocator.free(identifier);
            if (log.systemd_unit) |systemd_unit| allocator.free(systemd_unit);
            allocator.free(log.message);
        }
        allocator.free(log_entries);
    }

    try std.testing.expect(log_entries.len == 1);
    try std.testing.expectEqualStrings(entry.source, log_entries[0].source);
    try std.testing.expectEqualStrings(entry.unit.?, log_entries[0].unit.?);
    try std.testing.expectEqualStrings(entry.identifier.?, log_entries[0].identifier.?);
    try std.testing.expectEqualStrings(entry.systemd_unit.?, log_entries[0].systemd_unit.?);
    try std.testing.expect(entry.priority == log_entries[0].priority);
    try std.testing.expect(entry.pid.? == log_entries[0].pid.?);
}

test "Storage: retention cleanup" {
    const allocator = std.testing.allocator;
    var storage = try Storage.init(allocator, ":memory:");
    defer storage.deinit();

    // Insert some metrics with old timestamp
    const metrics = SystemMetrics{
        .cpu_percent = 50.0,
        .cpu_user = 30.0,
        .cpu_system = 20.0,
        .cpu_iowait = 5.0,
        .mem_total = 8000000000,
        .mem_used = 4000000000,
        .mem_percent = 50.0,
        .swap_total = 2000000000,
        .swap_used = 500000000,
    };

    // Insert metrics with a timestamp 10 seconds in the past
    const old_timestamp = std.time.timestamp() - 10;
    try storage.insertMetrics(old_timestamp, metrics);

    // Run retention (delete everything older than 5 seconds)
    try storage.runRetention(5);

    // Verify data was deleted
    const retrieved = try storage.getLatestMetrics();
    try std.testing.expect(retrieved == null);
}
