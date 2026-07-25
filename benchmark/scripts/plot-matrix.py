#!/usr/bin/env python3
# Generates summary plots from a benchmark scoreboard.csv.
#
# Reads:  <results-dir>/scoreboard.csv (columns: strategy, workload, write_ms_p50,
#         write_ms_p95, read_ms_p50, read_ms_p95, file_bytes, file_count, rpr, afd,
#         disqualified)
# Writes: <results-dir>/plots/{correctness, storage, read-latency, disqualified}.png
#
# Usage:  python3 plot-matrix.py <results-dir>

import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

UNSHREDDED = "unshredded"


def main(results_dir: str) -> int:
    results = Path(results_dir)
    csv_path = results / "scoreboard.csv"
    if not csv_path.is_file():
        print(f"FAIL: {csv_path} not found", file=sys.stderr)
        return 1

    df = pd.read_csv(csv_path)
    if df.empty:
        print(f"FAIL: {csv_path} has no rows", file=sys.stderr)
        return 1

    out_dir = results / "plots"
    out_dir.mkdir(exist_ok=True)

    plot_correctness(df, out_dir / "correctness.png")
    plot_storage(df, out_dir / "storage.png")
    plot_read_latency(df, out_dir / "read-latency.png")
    plot_disqualified(df, out_dir / "disqualified.png")

    print(f"Wrote 4 plots to {out_dir}")
    return 0


def plot_correctness(df: pd.DataFrame, out: Path) -> None:
    # Bar chart: % of rows lost (1 - RPR) per (workload, strategy). RPR=1 -> 0% loss.
    # Only meaningful for shredded strategies; unshredded is the baseline.
    work = df[df["strategy"] != UNSHREDDED].copy()
    if work.empty:
        write_placeholder(out, "no shredded cells")
        return
    work["row_loss_pct"] = (1.0 - work["rpr"].astype(float)) * 100.0
    pivot = work.pivot(index="workload", columns="strategy", values="row_loss_pct").fillna(0.0)
    fig, ax = plt.subplots(figsize=(11, 5))
    pivot.plot(kind="bar", ax=ax, width=0.85)
    ax.set_ylabel("Rows lost vs unshredded baseline (%)")
    ax.set_xlabel("Workload")
    ax.set_title("Correctness: row preservation by strategy (lower is better; 0% = no rows lost)")
    ax.axhline(0, color="black", linewidth=0.5)
    ax.legend(title="strategy", bbox_to_anchor=(1.02, 1), loc="upper left", fontsize="small")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    plt.savefig(out, dpi=140)
    plt.close(fig)


def plot_storage(df: pd.DataFrame, out: Path) -> None:
    # File bytes per strategy, normalized to unshredded for that workload.
    # 1.0 = same size as unshredded; <1.0 = shredded saves space; >1.0 = shredded costs space.
    baseline = (
        df[df["strategy"] == UNSHREDDED]
        .set_index("workload")["file_bytes"]
        .astype(float)
        .to_dict()
    )
    if not baseline:
        write_placeholder(out, "no unshredded baseline")
        return
    work = df.copy()
    work["base_bytes"] = work["workload"].map(baseline)
    work = work[work["base_bytes"].fillna(0) > 0]
    if work.empty:
        write_placeholder(out, "unshredded baseline has zero file_bytes")
        return
    work["bytes_normalized"] = work["file_bytes"].astype(float) / work["base_bytes"]
    pivot = work.pivot(index="workload", columns="strategy", values="bytes_normalized").fillna(0.0)
    fig, ax = plt.subplots(figsize=(11, 5))
    pivot.plot(kind="bar", ax=ax, width=0.85)
    ax.axhline(1.0, color="black", linewidth=0.7, linestyle="--", label="unshredded baseline")
    ax.set_ylabel("File bytes / unshredded baseline")
    ax.set_xlabel("Workload")
    ax.set_title("Storage cost relative to unshredded (lower is better)")
    ax.legend(title="strategy", bbox_to_anchor=(1.02, 1), loc="upper left", fontsize="small")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    plt.savefig(out, dpi=140)
    plt.close(fig)


def plot_read_latency(df: pd.DataFrame, out: Path) -> None:
    # read_ms_p50 per strategy per workload. Sum-of-reads (per score-matrix.sh layout).
    pivot = df.pivot(index="workload", columns="strategy", values="read_ms_p50").fillna(0.0)
    if pivot.empty:
        write_placeholder(out, "no read timings")
        return
    fig, ax = plt.subplots(figsize=(11, 5))
    pivot.plot(kind="bar", ax=ax, width=0.85)
    ax.set_ylabel("Read latency p50 (ms, sum of all read ops)")
    ax.set_xlabel("Workload")
    ax.set_title("Read wall-clock by strategy (lower is better)")
    ax.legend(title="strategy", bbox_to_anchor=(1.02, 1), loc="upper left", fontsize="small")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    plt.savefig(out, dpi=140)
    plt.close(fig)


def plot_disqualified(df: pd.DataFrame, out: Path) -> None:
    # Heatmap workload x strategy: 1 = disqualified (correctness violation), 0 = safe.
    work = df.copy()
    work["disq"] = (
        work["disqualified"].astype(str).str.lower().eq("true").astype(int)
    )
    pivot = work.pivot(index="workload", columns="strategy", values="disq").fillna(0).astype(int)
    if pivot.empty:
        write_placeholder(out, "no rows for heatmap")
        return
    fig, ax = plt.subplots(figsize=(9, 5))
    cmap = plt.get_cmap("RdYlGn_r")
    im = ax.imshow(pivot.values, cmap=cmap, vmin=0, vmax=1, aspect="auto")
    ax.set_xticks(np.arange(len(pivot.columns)))
    ax.set_yticks(np.arange(len(pivot.index)))
    ax.set_xticklabels(pivot.columns, rotation=30, ha="right")
    ax.set_yticklabels(pivot.index)
    for i in range(pivot.shape[0]):
        for j in range(pivot.shape[1]):
            label = "DQ" if pivot.values[i, j] else "OK"
            ax.text(j, i, label, ha="center", va="center", color="black", fontsize=9)
    ax.set_title("Strategy safety per workload (DQ = disqualified by RPR<1 or AFD>0)")
    plt.tight_layout()
    plt.savefig(out, dpi=140)
    plt.close(fig)


def write_placeholder(out: Path, reason: str) -> None:
    fig, ax = plt.subplots(figsize=(6, 2))
    ax.text(0.5, 0.5, f"no plot: {reason}", ha="center", va="center", fontsize=11)
    ax.axis("off")
    plt.savefig(out, dpi=120)
    plt.close(fig)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <results-dir>", file=sys.stderr)
        sys.exit(1)
    sys.exit(main(sys.argv[1]))
