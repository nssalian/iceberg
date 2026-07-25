#!/usr/bin/env python3
"""
Standalone per-column shred scorer.

Reads Parquet footer metadata directly (via pyarrow) - no gradle, no quick-check,
no timing runs. Computes the +1/0/-1 score per shredded field.

Scoring:
  +1  field was shredded, typed_value column has stats + fallback value column is fully null
  -1  field was shredded but fallback bytes present (partial shred) OR typed_value has no stats
   0  field COULD have been shredded but wasn't (needs unshredded baseline to detect)

Handles NESTED shredded objects (recurses into typed_value groups).

Usage:
  # Score a single warehouse dir (e.g., one strategy on one workload)
  ./aggregate-footer-scores.py --warehouse /tmp/iceberg-bench/warehouse-shred-v2/w5-clustered-v2-uniform-wilson \
      --workload w5-clustered --strategy v2-uniform-wilson

  # Score every cell in a warehouse base
  ./aggregate-footer-scores.py --warehouse-base /tmp/iceberg-bench/warehouse-shred-v2 \
      --output benchmark/results/scoreboard-columns.csv

  # With unshredded baseline for "0 = didn't shred" detection
  ./aggregate-footer-scores.py --warehouse-base /tmp/iceberg-bench/warehouse-shred-v2 \
      --unshredded-suffix -unshredded \
      --output benchmark/results/scoreboard-columns.csv
"""

import argparse
import csv
import glob
import os
import re
import sys
from collections import defaultdict

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("pyarrow is required. Install with: pip install pyarrow", file=sys.stderr)
    sys.exit(1)


VARIANT_TABLE_SUBPATH = "db/events_shredded/data"
UNSHREDDED_TABLE_SUBPATH = "db/events_variant/data"
# Snowflake Q6-Q11 corpus - written to arrays_shredded table by every workload's
# write-arrays-shredded op. Same variant metadata/value/typed_value schema as events_shredded.
ARRAYS_TABLE_SUBPATH = "db/arrays_shredded/data"


def is_variant_group(field):
    """A variant column is a GROUP with children including 'metadata' (BINARY) and 'value' (BINARY)."""
    if not hasattr(field, "flatten") and not str(field.type).startswith("struct"):
        return False
    child_names = _child_names(field)
    return "metadata" in child_names and "value" in child_names


def _child_names(field):
    """Return set of top-level child names for a pyarrow schema field (struct-typed)."""
    if not hasattr(field.type, "fields"):
        return set()
    return {sub.name for sub in field.type.fields}


def _get_child(field, name):
    if not hasattr(field.type, "fields"):
        return None
    for sub in field.type.fields:
        if sub.name == name:
            return sub
    return None


def walk_shredded_leaves(field, path_so_far):
    """Yield (dot_path, typed_field, value_field) for every primitive typed_value leaf under this variant field.

    path_so_far is the parquet column path (list of strings) to this variant column.
    Handles three shred shapes:
      1. Primitive shred: typed_value is a primitive (INT32, BINARY, etc). Yields one leaf.
      2. Nested object shred: typed_value is a struct with variant-shaped sub-fields
         (each sub-field has {value, typed_value}). Recurses per sub-field.
      3. Array shred (variant list<T>): typed_value is a LIST whose element is a variant-shaped
         struct {value, typed_value}. The physical parquet path adds `.list.element` between
         the parent and the recursed field. Nested lists (list<list<T>>) recurse naturally.
    """
    typed_group = _get_child(field, "typed_value")
    if typed_group is None:
        return
    # Array shred: variant list<T> - typed_value is LIST<struct{value,typed_value}>.
    # Recurse into the element (which itself has variant {value, typed_value} shape).
    # The physical parquet column path for list groups adds ".list.element" between
    # the parent typed_value and the element's own value/typed_value.
    if pa.types.is_list(typed_group.type) or pa.types.is_large_list(typed_group.type):
        element_field = typed_group.type.value_field
        yield from walk_shredded_leaves(
            element_field, path_so_far + ["typed_value", "list", "element"]
        )
        return
    # typed_value can be primitive (top-level primitive shredding) or a group
    if not hasattr(typed_group.type, "fields"):
        # top-level primitive shred - path is <path_so_far>.typed_value
        yield (path_so_far + ["typed_value"], typed_group, _get_child(field, "value"))
        return
    # nested object: iterate field groups
    for field_group in typed_group.type.fields:
        # Each field_group should have {value BINARY, typed_value <primitive-or-group>}
        if not hasattr(field_group.type, "fields"):
            continue
        fg_value = None
        fg_typed = None
        for sub in field_group.type.fields:
            if sub.name == "value":
                fg_value = sub
            elif sub.name == "typed_value":
                fg_typed = sub
        if fg_typed is None:
            continue
        new_path = path_so_far + ["typed_value", field_group.name]
        if pa.types.is_list(fg_typed.type) or pa.types.is_large_list(fg_typed.type):
            # nested variant array inside a nested object - recurse via array branch
            yield from walk_shredded_leaves(field_group, new_path)
        elif hasattr(fg_typed.type, "fields"):
            # nested object - recurse
            yield from walk_shredded_leaves(field_group, new_path)
        else:
            # primitive leaf
            yield (new_path + ["typed_value"], fg_typed, fg_value)


def score_parquet_file(path):
    """Return list of ColumnScore dicts for every shredded leaf in this parquet file."""
    pf = pq.ParquetFile(path)
    schema = pf.schema_arrow
    metadata = pf.metadata

    scores_by_field = {}  # key: dot_path -> aggregated score
    all_leaves = []

    for top_field in schema:
        if not is_variant_group(top_field):
            continue
        for leaf_path, typed_field, value_field in walk_shredded_leaves(
            top_field, [top_field.name]
        ):
            typed_col_path = ".".join(leaf_path)
            # value column path is same as typed_col_path but with last "typed_value" replaced by "value"
            value_col_path = ".".join(leaf_path[:-1] + ["value"]) if value_field is not None else None

            typed_nulls = 0
            typed_total = 0
            value_nulls = 0
            value_total = 0
            typed_has_stats = True
            # Distinguishes "all rows are null in typed_value" (legitimate: field absent for this
            # range) from "some rows are non-null but stats missing" (real regression). Without
            # this, an all-null shredded column scored -1 for "stats destroyed" even though the
            # writer correctly emits no min/max when everything is null.
            typed_all_rowgroups_all_null = True

            for rg_idx in range(metadata.num_row_groups):
                rg = metadata.row_group(rg_idx)
                for col_idx in range(rg.num_columns):
                    col = rg.column(col_idx)
                    col_path = col.path_in_schema
                    if col_path == typed_col_path:
                        typed_total += col.num_values
                        stats = col.statistics
                        rg_null_count = stats.null_count if stats is not None and stats.null_count is not None else 0
                        if rg_null_count != col.num_values:
                            typed_all_rowgroups_all_null = False
                        if stats is None or not stats.has_min_max:
                            # Missing min/max is fine if the row-group is entirely null.
                            if rg_null_count != col.num_values:
                                typed_has_stats = False
                        if stats is not None and stats.null_count is not None:
                            typed_nulls += stats.null_count
                    elif value_col_path is not None and col_path == value_col_path:
                        value_total += col.num_values
                        stats = col.statistics
                        if stats is not None and stats.null_count is not None:
                            value_nulls += stats.null_count

            no_fallbacks = value_col_path is None or value_nulls == value_total
            fallback_count = 0 if value_col_path is None else max(0, value_total - value_nulls)
            residual_fallback_count = fallback_count
            # Graded fallback score. present_rows = rows where the field was present in the source
            # (typed_value non-null + value non-null). fallback_rate = fallback / present_rows.
            # A near-zero fallback rate on a large column is functionally a success; the binary
            # +1/-1 rule punished 0.03% and 100% identically. Bands (graded (banded) scoring rule):
            #   fallback_rate < 0.01  -> +1  (near-perfect)
            #   fallback_rate < 0.10  ->  0  (marginal)
            #   fallback_rate >= 0.10 -> -1  (bad decision)
            typed_present = max(0, typed_total - typed_nulls)
            present_rows = typed_present + fallback_count
            fallback_rate = (fallback_count / present_rows) if present_rows > 0 else 0.0
            if typed_all_rowgroups_all_null and no_fallbacks:
                score = 0
                reason = "all-null typed column, no stats needed"
                null_reason = "all_null_no_data"
            elif no_fallbacks and typed_has_stats:
                score = 1
                reason = "shredded, no fallbacks, stats present"
                null_reason = "not_null"
            elif not no_fallbacks and not typed_has_stats:
                score = -1
                reason = "fallbacks present and stats missing/invalid"
                null_reason = "stats_destroyed"
            elif not no_fallbacks:
                if fallback_rate < 0.01:
                    score = 1
                    reason = f"near-perfect ({fallback_count} fallback / {present_rows} present = {fallback_rate:.4%})"
                    null_reason = "not_null"
                elif fallback_rate < 0.10:
                    score = 0
                    reason = f"marginal ({fallback_count} fallback / {present_rows} present = {fallback_rate:.4%})"
                    null_reason = "not_null"
                else:
                    score = -1
                    reason = f"fallbacks present ({fallback_count} non-null in value column, rate {fallback_rate:.2%})"
                    null_reason = "not_null"
            else:
                score = -1
                reason = "stats missing or invalid on typed_value"
                null_reason = "stats_destroyed"

            leaf_dot = ".".join(leaf_path[:-1])  # strip trailing typed_value; KEEP top-level name so multi-variant tables (arr_number vs arr_text) don't collide into one merged slot
            all_leaves.append({
                "field_path": leaf_dot,
                "score": score,
                "reason": reason,
                "null_reason": null_reason,
                "value_nulls": value_nulls,
                "value_total": value_total,
                "typed_nulls": typed_nulls,
                "typed_total": typed_total,
                "typed_has_stats": typed_has_stats,
                "typed_all_null": typed_all_rowgroups_all_null,
                "residual_fallback_count": residual_fallback_count,
                "fallback_rate": fallback_rate,
                "present_rows": present_rows,
            })
    return all_leaves


def merge_leaves(leaf_lists):
    """Merge per-file leaves across multiple parquet files in one cell.

    Sums null counts and totals across files, then re-scores at the cell level.
    """
    by_path = defaultdict(lambda: {
        "value_nulls": 0,
        "value_total": 0,
        "typed_nulls": 0,
        "typed_total": 0,
        "typed_has_stats": True,
        "typed_all_null": True,
        "residual_fallback_count": 0,
    })
    for file_leaves in leaf_lists:
        for leaf in file_leaves:
            slot = by_path[leaf["field_path"]]
            slot["value_nulls"] += leaf["value_nulls"]
            slot["value_total"] += leaf["value_total"]
            slot["typed_nulls"] += leaf["typed_nulls"]
            slot["typed_total"] += leaf["typed_total"]
            if not leaf["typed_has_stats"]:
                slot["typed_has_stats"] = False
            if not leaf.get("typed_all_null", True):
                slot["typed_all_null"] = False
            slot["residual_fallback_count"] += leaf.get("residual_fallback_count", 0)

    merged = []
    for path, slot in sorted(by_path.items()):
        no_fallbacks = slot["value_total"] == 0 or slot["value_nulls"] == slot["value_total"]
        if slot["typed_all_null"] and no_fallbacks:
            score = 0
            reason = "all-null typed column, no stats needed"
            null_reason = "all_null_no_data"
        elif no_fallbacks and slot["typed_has_stats"]:
            score = 1
            reason = "shredded, no fallbacks, stats present"
            null_reason = "not_null"
        elif not no_fallbacks:
            fallback_count = slot["value_total"] - slot["value_nulls"]
            typed_present = max(0, slot["typed_total"] - slot["typed_nulls"])
            present_rows = typed_present + fallback_count
            fallback_rate = (fallback_count / present_rows) if present_rows > 0 else 0.0
            if fallback_rate < 0.01:
                score = 1
                reason = f"near-perfect ({fallback_count} fallback / {present_rows} present = {fallback_rate:.4%})"
            elif fallback_rate < 0.10:
                score = 0
                reason = f"marginal ({fallback_count} fallback / {present_rows} present = {fallback_rate:.4%})"
            else:
                score = -1
                reason = f"fallbacks present ({fallback_count} non-null in value column, rate {fallback_rate:.2%})"
            null_reason = "not_null" if slot["typed_has_stats"] else "stats_destroyed"
        else:
            score = -1
            reason = "stats missing or invalid on typed_value"
            null_reason = "stats_destroyed"
        merged.append({
            "field_path": path,
            "score": score,
            "reason": reason,
            "null_reason": null_reason,
            **slot,
        })
    return merged


def detect_missed_opportunities(shredded_leaves, unshredded_dir):
    """Compare shredded cell against unshredded baseline to find 0-score fields.

    Returns list of dicts with score=0 for fields present in unshredded but not shredded.
    Best-effort - the unshredded variant column doesn't have primitive-typed columns to detect,
    so this returns [] unless a sidecar schema description is available.
    """
    # This is a placeholder. Truly detecting "field X was in the data but we didn't shred it"
    # requires either a workload spec file or field-frequency analysis on the unshredded data.
    # Neither exists in the current pipeline. We emit a note in the CSV instead.
    return []


def cell_scoreboard_row(strategy, workload, merged):
    aggregate = sum(m["score"] for m in merged)
    fields_str = " | ".join(f"{m['field_path']}={m['score']:+d}" for m in merged)
    reasons_str = " || ".join(f"{m['field_path']}: {m['reason']}" for m in merged)
    null_reasons_str = " || ".join(
        f"{m['field_path']}: {m.get('null_reason', 'not_null')}" for m in merged
    )
    residual_str = " || ".join(
        f"{m['field_path']}: {m.get('residual_fallback_count', 0)}" for m in merged
    )
    # Footer-only null-count gate approximation. We cannot compare shredded vs unshredded at
    # query time from footers alone; instead flag leaves where the shredded typed column is
    # entirely null but the value column has non-null bytes (type-drift silent-corruption
    # smoke test). Real query-time gate is a follow-up.
    # TODO: null-count equality gate requires query-time comparison; footer approximation
    # implemented; full gate in follow-up
    null_gate_fails = 0
    null_gate_reason_parts = []
    for m in merged:
        typed_all_null = m.get("typed_all_null", False)
        val_non_null = m.get("value_total", 0) - m.get("value_nulls", 0)
        if typed_all_null and val_non_null > 0:
            null_gate_fails += 1
            null_gate_reason_parts.append(
                f"{m['field_path']}: typed all-null but value has {val_non_null} non-null"
            )
    return {
        "strategy": strategy,
        "workload": workload,
        "shredded_leaves": len(merged),
        "aggregate_score": aggregate,
        "plus_ones": sum(1 for m in merged if m["score"] == 1),
        "minus_ones": sum(1 for m in merged if m["score"] == -1),
        "zero_all_null": sum(1 for m in merged if m.get("null_reason") == "all_null_no_data"),
        "residual_fallback_total": sum(m.get("residual_fallback_count", 0) for m in merged),
        "null_gate_fails": null_gate_fails,
        "null_gate_reasons": " || ".join(null_gate_reason_parts),
        "fields": fields_str,
        "reasons": reasons_str,
        "null_reason": null_reasons_str,
        "residual_fallback_count": residual_str,
    }


CELL_RE = re.compile(r"^(?P<workload>.+)-(?P<strategy>b1-majority|b4-first-row|b5-first-20-uniform|v2-uniform-wilson|v2-uniform|v2-cardgated|unshredded)$")


def enumerate_cells(warehouse_base):
    """Yield (cell_name, warehouse_dir, workload, strategy) for every cell dir under warehouse_base."""
    for entry in sorted(os.listdir(warehouse_base)):
        full = os.path.join(warehouse_base, entry)
        if not os.path.isdir(full):
            continue
        match = CELL_RE.match(entry)
        if not match:
            continue
        yield entry, full, match.group("workload"), match.group("strategy")


def score_warehouse(warehouse_dir, workload, strategy):
    """Score every parquet file in a warehouse cell and return a scoreboard row + raw merged leaves.

    Scans both events_shredded (workload variant column) and arrays_shredded (SnowflakeQ6-Q11
    corpus). For the unshredded strategy the events table is events_variant. Returns None only
    when the strategy's expected table has no data on disk.
    """
    all_leaves = []
    events_subpath = UNSHREDDED_TABLE_SUBPATH if strategy == "unshredded" else VARIANT_TABLE_SUBPATH
    for subpath, table_tag in ((events_subpath, "events"), (ARRAYS_TABLE_SUBPATH, "arrays")):
        data_dir = os.path.join(warehouse_dir, subpath)
        files = sorted(glob.glob(os.path.join(data_dir, "*.parquet")))
        if not files:
            continue
        leaf_lists = []
        for pf in files:
            try:
                leaves = score_parquet_file(pf)
                if leaves:
                    for leaf in leaves:
                        leaf["table"] = table_tag
                    leaf_lists.append(leaves)
            except Exception as exc:
                print(f"error scoring {pf}: {exc}", file=sys.stderr)
        if leaf_lists:
            all_leaves.extend(merge_leaves(leaf_lists))
    if not all_leaves:
        # For unshredded the events_variant table has no typed_value columns so leaves is empty.
        # Emit an explicit zero-shredded row so the baseline is present in the scoreboard.
        if strategy == "unshredded":
            events_dir = os.path.join(warehouse_dir, events_subpath)
            if glob.glob(os.path.join(events_dir, "*.parquet")):
                return cell_scoreboard_row(strategy, workload, []), []
        return None, []
    return cell_scoreboard_row(strategy, workload, all_leaves), all_leaves


def apply_missed_opportunities(rows_by_key, leaves_by_key):
    """Add 0-scores for fields that were shredded by SOME strategy for a workload but not this one.

    rows_by_key:   {(strategy, workload): scoreboard_row}
    leaves_by_key: {(strategy, workload): [merged leaves]}

    Mutates rows_by_key to include a "missed" count column and updates aggregate to be a
    per-strategy view. Fields at 0 do not change the +1/-1 aggregate (the per-column scoring rule:
    0 = didn't shred, not a wrong decision), but the count of misses is reported.
    """
    # Group cells by workload
    by_workload = defaultdict(list)
    for (strategy, workload) in leaves_by_key:
        by_workload[workload].append(strategy)

    for workload, strategies in by_workload.items():
        # Union of all field_paths shredded by ANY strategy for this workload
        all_fields = set()
        per_strategy_fields = {}
        for strategy in strategies:
            fields = {leaf["field_path"] for leaf in leaves_by_key[(strategy, workload)]}
            per_strategy_fields[strategy] = fields
            all_fields.update(fields)

        # For each cell, compute missed count and add to the row
        for strategy in strategies:
            missed = all_fields - per_strategy_fields[strategy]
            row = rows_by_key[(strategy, workload)]
            row["missed_shreds"] = len(missed)
            row["missed_fields"] = " | ".join(sorted(missed)) if missed else ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", help="score a single warehouse cell directory")
    ap.add_argument("--workload", help="workload label for --warehouse mode")
    ap.add_argument("--strategy", help="strategy label for --warehouse mode")
    ap.add_argument("--warehouse-base", help="score every cell under this base dir")
    ap.add_argument("--output", "-o", help="output CSV path (required for --warehouse-base)")
    args = ap.parse_args()

    rows_by_key = {}
    leaves_by_key = {}

    if args.warehouse:
        if not args.workload or not args.strategy:
            print("--warehouse requires --workload and --strategy", file=sys.stderr)
            sys.exit(1)
        row, leaves = score_warehouse(args.warehouse, args.workload, args.strategy)
        if row is None:
            print(f"no parquet files found under {args.warehouse}", file=sys.stderr)
            sys.exit(2)
        key = (args.strategy, args.workload)
        rows_by_key[key] = row
        leaves_by_key[key] = leaves
    elif args.warehouse_base:
        if not args.output:
            print("--warehouse-base requires --output", file=sys.stderr)
            sys.exit(1)
        if not os.path.isdir(args.warehouse_base):
            print(f"not a directory: {args.warehouse_base}", file=sys.stderr)
            sys.exit(1)
        for cell, cell_dir, workload, strategy in enumerate_cells(args.warehouse_base):
            row, leaves = score_warehouse(cell_dir, workload, strategy)
            if row is not None:
                key = (strategy, workload)
                rows_by_key[key] = row
                leaves_by_key[key] = leaves
                print(f"scored {cell}: aggregate={row['aggregate_score']} (+{row['plus_ones']}/-{row['minus_ones']})")
    else:
        print("must specify --warehouse or --warehouse-base", file=sys.stderr)
        sys.exit(1)

    # Cross-strategy comparison to detect "0 = didn't shred" missed opportunities.
    apply_missed_opportunities(rows_by_key, leaves_by_key)

    rows = [rows_by_key[key] for key in sorted(rows_by_key)]

    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)) or ".", exist_ok=True)
        with open(args.output, "w", newline="") as fh:
            fieldnames = [
                "strategy", "workload", "shredded_leaves", "aggregate_score",
                "plus_ones", "minus_ones", "zero_all_null", "residual_fallback_total",
                "null_gate_fails", "missed_shreds", "fields", "reasons",
                "null_reason", "residual_fallback_count", "null_gate_reasons",
                "missed_fields",
            ]
            writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        print(f"wrote {args.output} ({len(rows)} rows)")
    else:
        # print to stdout for single-cell mode
        for row in rows:
            print(f"strategy={row['strategy']} workload={row['workload']}")
            print(f"  shredded leaves: {row['shredded_leaves']}")
            print(f"  aggregate score: {row['aggregate_score']} (+{row['plus_ones']} / -{row['minus_ones']})")
            missed = row.get("missed_shreds", 0)
            if missed:
                print(f"  missed shreds (0-score): {missed}")
            print(f"  fields: {row['fields']}")


if __name__ == "__main__":
    main()
