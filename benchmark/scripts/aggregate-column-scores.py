#!/usr/bin/env python3
"""
Aggregate Per-column shred scores from quick-check CSVs into a matrix scoreboard.

Reads:  benchmark/results/quickcheck-*-scores.csv (one per (workload,strategy) quick-check run)
Writes: <results-dir>/scoreboard-columns.csv  with columns:
        strategy, workload, shredded_cols, aggregate_score, fields, reasons

Usage:  python3 benchmark/scripts/aggregate-column-scores.py <results-dir> [<quickcheck-glob>]

Defaults <quickcheck-glob> to benchmark/results/quickcheck-*-scores.csv.
"""

import csv
import glob
import os
import sys
from collections import defaultdict


def main():
    if len(sys.argv) < 2:
        print("usage: aggregate-column-scores.py <results-dir> [<quickcheck-glob>]", file=sys.stderr)
        sys.exit(1)
    results_dir = sys.argv[1]
    pattern = sys.argv[2] if len(sys.argv) >= 3 else "benchmark/results/quickcheck-*-scores.csv"

    cells = defaultdict(lambda: {"fields": [], "reasons": [], "aggregate": None})

    for csv_path in sorted(glob.glob(pattern)):
        try:
            with open(csv_path, newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    workload = row.get("workload", "")
                    strategy = row.get("strategy", "")
                    field = row.get("shredded_field", "")
                    try:
                        score = int(row.get("score", "0"))
                    except ValueError:
                        continue
                    reason = row.get("reason", "")
                    key = (strategy, workload)
                    if field == "AGGREGATE":
                        cells[key]["aggregate"] = score
                    else:
                        cells[key]["fields"].append(f"{field}={score:+d}")
                        cells[key]["reasons"].append(f"{field}: {reason}")
        except OSError as exc:
            print(f"skip {csv_path}: {exc}", file=sys.stderr)

    out_path = os.path.join(results_dir, "scoreboard-quickcheck.csv")
    os.makedirs(results_dir, exist_ok=True)
    with open(out_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["strategy", "workload", "shredded_cols", "aggregate_score", "fields", "reasons"])
        for (strategy, workload), info in sorted(cells.items()):
            writer.writerow([
                strategy,
                workload,
                len(info["fields"]),
                info["aggregate"] if info["aggregate"] is not None else sum(
                    int(f.split("=")[1]) for f in info["fields"]
                ),
                " | ".join(info["fields"]),
                " || ".join(info["reasons"]),
            ])
    print(f"wrote {out_path} ({len(cells)} cells)")


if __name__ == "__main__":
    main()
