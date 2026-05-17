# /// script
# requires-python = ">=3.10"
# ///
"""Analyze a sermon-agent RSS sample series for buffer-pool / heap leaks.

Usage: analyze_rss.py <samples_csv> <warmup_cycles> <ceiling_mb> [slope_kb_per_cycle]

  samples_csv:        one "<cycle>,<rss_kb>" line per collection cycle, as
                      written by buffer_pool_soak.sh
  warmup_cycles:      leading cycles to drop before the slope is measured.
                      The DuckDB buffer pool warms for hundreds of cycles;
                      its slope during warmup is not a leak.
  ceiling_mb:         coarse, mode-specific safety net. Peak RSS over the
                      whole run must stay below this. Catches a gross
                      runaway only - the slope is the precise gate.
  slope_kb_per_cycle: the regression gate. If given, the linear slope of
                      the post-warmup region must stay below it. Omit (or
                      pass a negative value) to report the slope only.

Why the slope is the gate, not an absolute ceiling:

  The daemon bounds DuckDB's working set with PRAGMA memory_limit='128MB',
  but that pragma governs DuckDB's buffer manager, not process RSS. RSS
  also carries allocator overhead and pages glibc has not returned to the
  OS. Measured over a 2600-cycle clean run, RSS climbs the whole time -
  from ~42 MB past 200 MB - and never plateaus. So there is no flat
  steady-state RSS to anchor an absolute ceiling against.

  What is stable is the post-warmup *slope*: once the buffer pool has
  warmed, clean code rises at a measured ~40-61 KB/cycle. A genuine leak -
  the PR #96 buffer-pool leak that grew RSS ~5-9 MB/hour - adds a per-cycle
  increment on top of that, so it shows up as a steeper slope. The bench
  fails when the post-warmup slope exceeds a threshold set well above the
  measured clean-code slope.

Exit code is 0 on pass, 1 on a threshold breach, 2 on bad input.
"""
from __future__ import annotations

import sys


def load(path: str) -> list[int]:
    rss: list[int] = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("n,") or line.startswith("#"):
                continue
            parts = line.split(",")
            if len(parts) != 2:
                continue
            try:
                rss.append(int(parts[1]))
            except ValueError:
                continue
    return rss


def linfit_slope(ys: list[int]) -> float:
    """Least-squares slope of ys against index, in the units of ys per step."""
    m = len(ys)
    if m < 2:
        return 0.0
    xs = range(m)
    sx = sum(xs)
    sy = sum(ys)
    sxx = sum(x * x for x in xs)
    sxy = sum(x * y for x, y in zip(xs, ys))
    denom = m * sxx - sx * sx
    if denom == 0:
        return 0.0
    return (m * sxy - sx * sy) / denom


def main(argv: list[str]) -> int:
    if len(argv) < 4:
        print(__doc__, file=sys.stderr)
        return 2

    samples_csv = argv[1]
    warmup_cycles = int(argv[2])
    ceiling_kb = float(argv[3]) * 1024.0
    slope_gate = float(argv[4]) if len(argv) > 4 else -1.0

    rss = load(samples_csv)
    if len(rss) < warmup_cycles + 2:
        print(
            f"FAIL: only {len(rss)} samples, need > warmup ({warmup_cycles}) + 2",
            file=sys.stderr,
        )
        return 2

    peak_kb = max(rss)
    measured = rss[warmup_cycles:]
    slope = linfit_slope(measured)  # KB per cycle
    first_mb = sum(measured[: len(measured) // 4]) / (len(measured) // 4) / 1024
    last_mb = sum(measured[-len(measured) // 4 :]) / (len(measured) // 4) / 1024

    print(f"samples            : {len(rss)} cycles")
    print(f"warmup dropped     : {warmup_cycles} cycles")
    print(f"peak RSS           : {peak_kb / 1024:.1f} MB (ceiling {ceiling_kb / 1024:.0f} MB)")
    print(f"post-warmup slope  : {slope:.1f} KB/cycle")
    print(f"post-warmup band   : {first_mb:.1f} MB -> {last_mb:.1f} MB")

    failed = False
    if peak_kb > ceiling_kb:
        print(
            f"FAIL: peak RSS {peak_kb / 1024:.1f} MB exceeds the "
            f"{ceiling_kb / 1024:.0f} MB safety net - gross buffer-pool / "
            f"heap runaway"
        )
        failed = True
    if slope_gate >= 0 and slope > slope_gate:
        print(
            f"FAIL: post-warmup slope {slope:.1f} KB/cycle exceeds "
            f"{slope_gate:.0f} KB/cycle - sustained growth past buffer-pool warmup"
        )
        failed = True

    if failed:
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
