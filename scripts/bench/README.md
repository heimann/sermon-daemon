# scripts/bench

This directory holds two regression harnesses:

- the **CPU harness** (`measure_cpu.sh` + `compare_builds.py` +
  `populate_metrics_db.py`), documented immediately below; and
- the **buffer-pool RSS harness** (`buffer_pool_soak.sh` +
  `analyze_rss.py`), documented near the end of this file.

## CPU regression harness

Daemon CPU regression harness. Pairs with the v0.0.1-rc7 fix - the
daemon used to open and close DuckDB on every collection cycle, which
pegged a CPU core on populated databases. We caught it because David
asked his agent "what's been happening on contabo" and the answer
showed sermon-agent itself burning ~40% CPU.

The pieces:

- **`populate_metrics_db.py`** - bulk-inserts ~7 days of synthetic
  metrics + processes + disks into a fresh DuckDB so it grows to
  contabo-shaped size (~400 MB). Without this, fresh-VM measurements
  understate the bug because a 12 KB DB opens and closes essentially
  for free.

- **`measure_cpu.sh`** - starts a daemon binary against a given DB,
  samples `%cpu` and RSS once per second for N seconds, writes one
  sample per line to an output file.

- **`compare_builds.py`** - reads two sample files, prints a summary
  table (mean / p50 / p99 / max / RSS) and a CPU delta.

## Running it

The bench needs a Linux box with `bash`, `python3`, `uv`, and the
release tarballs you want to compare. exe.dev VMs work; voyager works
if you've installed the daemon's runtime deps.

```fish
# 1. Populate a contabo-shaped DB
uv run --script populate_metrics_db.py /tmp/metrics.db # ~400 MB

# 2. Measure unpatched (current public release)
mkdir -p /tmp/bench-rc6
cd /tmp/bench-rc6
curl -fsSL https://github.com/heimann/sermon-daemon/releases/download/v0.0.1-rc6/sermon-v0.0.1-rc6-x86_64-linux-gnu.tar.gz -o release.tgz
tar xzf release.tgz
cd -
bash measure_cpu.sh /tmp/bench-rc6/sermon-v0.0.1-rc6-x86_64-linux-gnu /tmp/cpu_unpatched.txt 90 /tmp/metrics.db

# 3. Build + measure patched (HEAD)
cd /path/to/sermon-daemon && LD_LIBRARY_PATH=lib zig build
mkdir -p /tmp/bench-patched/{bin,lib}
cp zig-out/bin/sermon-agent /tmp/bench-patched/bin/
cp lib/libduckdb.so /tmp/bench-patched/lib/
bash measure_cpu.sh /tmp/bench-patched /tmp/cpu_patched.txt 90 /tmp/metrics.db

# 4. Compare
python compare_builds.py /tmp/cpu_unpatched.txt /tmp/cpu_patched.txt UNPATCHED PATCHED
```

Expected output for PR #86:

```
label          n    mean    p50    p99    max   RSS_MB
UNPATCHED     90   2.04%   1.8%   5.8%   5.8%       53
PATCHED       90   0.54%   0.4%   2.1%   2.1%       48

CPU reduction: 73% (2.04% -> 0.54%)
```

## Limitations + future work

The current bench reproduces the dominant DuckDB-open/close cost but
does **not** reproduce contabo's full pathology. Contabo's running
unpatched daemon shows 100% CPU; a fresh VM with the same DB shape
shows only 2%. The remaining 50x gap is some combination of:

- accumulated long-running daemon state (heap fragmentation, allocator
  churn, FD growth)
- process count multiplier (contabo has ~200 PIDs; an idle exe.dev VM
  has ~80)
- VPS storage I/O slower than exe.dev's local SSD

Once PR #86 is deployed and contabo updates to the patched daemon, we
should redo the strace + measurement on contabo to see what residual
remains. If it's > 5%, extend this harness with:

- A long-soak mode (24h+) to surface accumulated state issues
- A fake-process generator (spawn N idle bash sleeps before measuring)
  to simulate contabo's process count

## Buffer-pool RSS regression harness

Pairs with PR #96, which leaked the DuckDB buffer pool and grew daemon
RSS ~5-9 MB/hour over a multi-hour run. No automated test caught it.
This harness exercises the daemon's collection / buffer-pool loop over
many cycles and fails if resident memory grows past a ceiling.

The pieces:

- **`buffer_pool_soak.sh`** - starts the daemon at the 1s minimum
  interval, samples `VmRSS` once per second into a CSV, then hands the
  CSV to `analyze_rss.py`. The sampler and the daemon's collection loop
  are not phase-locked, so a sample is one ~1s reading rather than
  exactly one collection cycle. Two modes via `BENCH_MODE`.
- **`analyze_rss.py`** - reads the RSS series, drops a warmup prefix,
  and applies the ceiling and slope gates. Exit 0 on pass, 1 on a
  threshold breach, 2 on bad input.

### Running it

```fish
# fast mode (default): ~45 cycles, completes in well under a minute
LD_LIBRARY_PATH=lib zig build bench-buffer-pool
# or: mise run bench-buffer-pool

# soak mode: 1200 cycles (~20 min), the real regression gate
BENCH_MODE=soak LD_LIBRARY_PATH=lib zig build bench-buffer-pool
```

Knobs (env vars): `BENCH_MODE` (`fast` | `soak`), `BENCH_CYCLES`,
`BENCH_WARMUP`, `BENCH_CEILING_MB`, `BENCH_SLOPE_KB`.

### How the thresholds were derived

The daemon bounds DuckDB's working set with `PRAGMA
memory_limit='128MB'`, but that pragma governs DuckDB's buffer manager,
not process RSS. RSS also carries allocator overhead and pages glibc
has not returned to the OS.

The measurement that drove the design: a single 2600-cycle clean run
(n=1) of current `origin/main` code. In that run RSS did **not**
plateau - it climbed the whole time, from ~42 MB at start past **207
MB** at cycle 2600, with 200-cycle window means stepping 55, 76, 91,
101, 109, 114, 120, 134, 142, 150, 160, 168 MB. That one run showed no
flat steady-state RSS, which is why the gate is built on the slope
rather than an absolute ceiling: a fixed ceiling that passes a short
run would be breached by a long one. This is a single observation, not
a fully characterised distribution - the slope gates below are set
with wide margins partly to absorb that uncertainty.

What looks stable is the post-warmup *slope*. Once the buffer pool has
warmed (after ~900 cycles) clean code rose at a measured 40-61
KB/sample across runs. A genuine leak - PR #96's buffer-pool leak that
grew RSS ~5-9 MB/hour - adds a per-sample increment on top, so it shows
up as a steeper slope. The slope is the regression gate; the peak-RSS
ceiling is only a coarse, mode-specific safety net for a gross runaway.

Thresholds (all set from measurement, not guessed):

- **Fast-mode slope gate = 600 KB/sample.** Over 45 cycles the daemon is
  still in buffer-pool warmup; the warmup slope measured 151-225
  KB/sample across clean runs. 600 KB/sample is ~2.7x that clean
  ceiling, so fast mode catches a leak of roughly >= 375 KB/sample.
- **Fast-mode ceiling = 90 MB.** Clean-code peak RSS over the first 45
  cycles measured 48.5-50.5 MB; 90 MB is a ~1.8x safety net.
- **Soak-mode slope gate = 300 KB/sample.** Measured post-warmup
  (cycles 900+) clean-code slope is 40-61 KB/sample; 300 KB/sample is
  ~5x that. This is the real regression gate.
- **Soak-mode ceiling = 200 MB.** Clean-code peak RSS over 1200 cycles
  measured 130 MB; 200 MB is a ~1.5x safety net.

Fast mode is a CI smoke test, like the existing `bench.sh`. It cannot
catch a PR #96-scale leak (~1.4-2.5 KB/sample at 1s sampling - below
measurement noise); soak mode, where the pool has warmed and the slope
is stable, is the real gate.

### Leak-injection proof

The bench was verified to catch a leak by temporarily injecting a
synthetic per-cycle allocation that is never freed (a `memset` 1 MB
buffer at the top of the collection loop in `src/agent/main.zig`).
With the leak in:

- fast mode failed - post-warmup slope 1047 KB/sample vs the 600
  KB/sample gate (clean code: ~200 KB/sample);
- a soak-style run failed both gates - slope 973 KB/sample vs the 300
  KB/sample gate, and peak RSS 280 MB vs the 200 MB safety net.

The injection was then reverted; the committed daemon code has no
leak and both modes pass clean.
