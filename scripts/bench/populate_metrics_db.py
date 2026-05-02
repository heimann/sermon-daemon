# /// script
# requires-python = ">=3.10"
# dependencies = ["duckdb>=1.2.1"]
# ///
"""Populate a sermon metrics.db with synthetic data to roughly contabo's
size (~300MB), so we can measure unpatched-vs-patched daemon CPU on a
DuckDB file that's actually heavy to mmap."""
import duckdb
import sys
import time

if len(sys.argv) < 2:
    print("usage: populate_metrics_db.py <path>")
    sys.exit(1)

db_path = sys.argv[1]
con = duckdb.connect(db_path)

# Create the same schema the daemon would create.
con.execute("""
CREATE TABLE IF NOT EXISTS metrics (
  timestamp TIMESTAMP NOT NULL,
  cpu_percent REAL,
  cpu_user REAL,
  cpu_system REAL,
  cpu_iowait REAL,
  mem_total BIGINT,
  mem_used BIGINT,
  mem_percent REAL,
  swap_total BIGINT,
  swap_used BIGINT
);
""")
con.execute("CREATE INDEX IF NOT EXISTS idx_metrics_ts ON metrics(timestamp);")

con.execute("""
CREATE TABLE IF NOT EXISTS processes (
  timestamp TIMESTAMP NOT NULL,
  pid INTEGER,
  name VARCHAR,
  cmdline VARCHAR,
  state CHAR(1),
  cpu_percent REAL,
  mem_rss BIGINT,
  threads INTEGER,
  username VARCHAR
);
""")
con.execute("CREATE INDEX IF NOT EXISTS idx_processes_ts ON processes(timestamp);")
con.execute("CREATE INDEX IF NOT EXISTS idx_processes_name ON processes(name);")

con.execute("""
CREATE TABLE IF NOT EXISTS disks (
  timestamp TIMESTAMP NOT NULL,
  mount_point VARCHAR,
  filesystem VARCHAR,
  total_bytes BIGINT,
  used_bytes BIGINT,
  percent REAL
);
""")

con.execute("""
CREATE TABLE IF NOT EXISTS logs (
  timestamp TIMESTAMP NOT NULL,
  source VARCHAR,
  unit VARCHAR,
  priority INTEGER,
  message TEXT,
  pid INTEGER
);
""")

# 7 days of data at 10s cadence.
print("seeding metrics (60480 rows)...")
con.execute("""
INSERT INTO metrics
SELECT
  TIMESTAMP '2026-04-25 00:00:00' + INTERVAL (i*10) SECOND as timestamp,
  20.0 + (i % 10) as cpu_percent,
  10.0 as cpu_user, 5.0 as cpu_system, 0.5 as cpu_iowait,
  16000000000 as mem_total,
  1500000000 + (i % 100000000) as mem_used,
  10.0 as mem_percent,
  0 as swap_total, 0 as swap_used
FROM range(0, 60480) t(i);
""")

# 200 processes per sample × 60480 samples = ~12M rows
print("seeding processes (12M rows, this takes a minute)...")
con.execute("""
INSERT INTO processes
SELECT
  TIMESTAMP '2026-04-25 00:00:00' + INTERVAL (sample*10) SECOND as timestamp,
  100 + pid_offset as pid,
  'process-' || (pid_offset % 50) as name,
  '/usr/bin/process-' || (pid_offset % 50) || ' --flag=' || sample as cmdline,
  'S' as state,
  CAST((pid_offset % 100) AS REAL) as cpu_percent,
  100000000 + pid_offset * 1000 as mem_rss,
  4 + (pid_offset % 8) as threads,
  CASE WHEN pid_offset % 5 = 0 THEN 'root' ELSE 'dmeh' END as username
FROM range(0, 60480) s(sample), range(0, 200) p(pid_offset);
""")

# Disks: ~5 per sample
print("seeding disks (300K rows)...")
con.execute("""
INSERT INTO disks
SELECT
  TIMESTAMP '2026-04-25 00:00:00' + INTERVAL (sample*10) SECOND as timestamp,
  '/mount-' || disk_idx as mount_point,
  'ext4' as filesystem,
  100000000000 as total_bytes,
  50000000000 + (sample * 1000) as used_bytes,
  50.0 + (sample % 30) as percent
FROM range(0, 60480) s(sample), range(0, 5) d(disk_idx);
""")

# Force a checkpoint so the on-disk file matches the inserted data
con.execute("CHECKPOINT;")
con.close()
print("done")
