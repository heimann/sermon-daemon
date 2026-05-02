# scripts/bench

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
