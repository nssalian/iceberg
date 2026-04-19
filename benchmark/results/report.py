#!/usr/bin/env python3
"""Generate comparison reports from benchmark results.

Usage: report.py --input <results_dir> --output <output_dir>

Reads summary.csv and produces:
  - summary_stats.csv: per-operation median, mean, stddev
  - comparison by engine, format, operation
"""

import argparse
import csv
import os
import statistics
import sys
from collections import defaultdict


def load_data(csv_path):
    """Load summary.csv into a list of dicts."""
    rows = []
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["wall_clock_ms"] = float(row["wall_clock_ms"])
            row["iteration"] = int(row["iteration"])
            rows.append(row)
    return rows


def compute_stats(rows):
    """Group by (engine, operation, format, compression) and compute stats."""
    groups = defaultdict(list)
    for row in rows:
        key = (row["engine"], row["operation"], row["format"], row["compression"])
        groups[key].append(row["wall_clock_ms"])

    stats = []
    for (engine, operation, fmt, comp), timings in sorted(groups.items()):
        entry = {
            "engine": engine,
            "operation": operation,
            "format": fmt,
            "compression": comp,
            "count": len(timings),
            "median_ms": round(statistics.median(timings), 1),
            "mean_ms": round(statistics.mean(timings), 1),
            "min_ms": round(min(timings), 1),
            "max_ms": round(max(timings), 1),
        }
        if len(timings) > 1:
            entry["stddev_ms"] = round(statistics.stdev(timings), 1)
        else:
            entry["stddev_ms"] = 0.0
        stats.append(entry)
    return stats


def write_stats_csv(stats, output_path):
    """Write aggregated stats to CSV."""
    if not stats:
        print("No data to write.", file=sys.stderr)
        return

    fields = ["engine", "operation", "format", "compression",
              "count", "median_ms", "mean_ms", "min_ms", "max_ms", "stddev_ms"]
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(stats)


def print_comparison(stats):
    """Print a human-readable comparison table to stdout."""
    print("\n=== Benchmark Results Summary ===\n")
    print(f"{'Engine':<15} {'Operation':<30} {'Format':<10} {'Median (ms)':<15} {'StdDev':<10}")
    print("-" * 80)
    for s in stats:
        print(
            f"{s['engine']:<15} {s['operation']:<30} {s['format']:<10} "
            f"{s['median_ms']:<15} {s['stddev_ms']:<10}"
        )

    # Cross-engine comparison
    by_op = defaultdict(list)
    for s in stats:
        by_op[s["operation"]].append(s)

    print("\n=== Cross-Engine Comparison ===\n")
    for op, entries in sorted(by_op.items()):
        if len(entries) > 1:
            engines = ", ".join(
                f"{e['engine']}={e['median_ms']}ms" for e in entries
            )
            print(f"  {op}: {engines}")


def main():
    parser = argparse.ArgumentParser(description="Benchmark report generator")
    parser.add_argument("--input", required=True, help="Results directory containing summary.csv")
    parser.add_argument("--output", required=True, help="Output directory for reports")
    args = parser.parse_args()

    csv_path = os.path.join(args.input, "summary.csv")
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found. Run collect-metrics.sh first.", file=sys.stderr)
        sys.exit(1)

    os.makedirs(args.output, exist_ok=True)

    rows = load_data(csv_path)
    stats = compute_stats(rows)

    stats_path = os.path.join(args.output, "summary_stats.csv")
    write_stats_csv(stats, stats_path)
    print(f"Stats written to: {stats_path}")

    print_comparison(stats)


if __name__ == "__main__":
    main()
