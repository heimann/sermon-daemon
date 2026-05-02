"""Compare CPU sample CSVs from measure_cpu.sh runs of two builds.

Usage: python compare_builds.py <unpatched.txt> <patched.txt> [<label_a> <label_b>]

Each input file is one sample per line: "<cpu%> <rss_kb>".
"""
from __future__ import annotations

import sys
from pathlib import Path


def stats(path: Path, label: str) -> tuple[str, int, float, float, float, float, float]:
    lines = path.read_text().strip().splitlines()
    cpus = [float(l.split()[0]) for l in lines if l.strip()]
    rss = [int(l.split()[1]) for l in lines if l.strip()]
    n = len(cpus)
    if n == 0:
        raise SystemExit(f"no samples in {path}")
    return (
        label,
        n,
        sum(cpus) / n,
        sorted(cpus)[n // 2],
        sorted(cpus)[int(n * 0.99)],
        max(cpus),
        max(rss) / 1024,
    )


def main(argv: list[str]) -> int:
    if len(argv) < 3:
        print(__doc__, file=sys.stderr)
        return 2

    a_path = Path(argv[1])
    b_path = Path(argv[2])
    a_label = argv[3] if len(argv) > 3 else "A"
    b_label = argv[4] if len(argv) > 4 else "B"

    rows = [stats(a_path, a_label), stats(b_path, b_label)]

    print(f"{'label':<12}  {'n':>3}  {'mean':>6}  {'p50':>5}  {'p99':>5}  {'max':>5}  {'RSS_MB':>7}")
    for label, n, mean, p50, p99, mx, rss in rows:
        print(f"{label:<12}  {n:>3}  {mean:>5.2f}%  {p50:>4.1f}%  {p99:>4.1f}%  {mx:>4.1f}%  {rss:>7.0f}")

    a_mean = rows[0][2]
    b_mean = rows[1][2]
    if a_mean > 0:
        delta_pct = (a_mean - b_mean) / a_mean * 100
        sign = "reduction" if delta_pct > 0 else "regression"
        print(f"\nCPU {sign}: {abs(delta_pct):.0f}% ({a_mean:.2f}% -> {b_mean:.2f}%)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
