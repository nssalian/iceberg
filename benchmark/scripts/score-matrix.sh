#!/usr/bin/env bash
# Aggregates per-cell results into scoreboard.csv + pareto.txt + schemas.txt.
# Pure bash + awk + jq; no matplotlib, no Python.
#
# Usage:
#   ./benchmark/scripts/score-matrix.sh <results-dir>
#
# Reads from <results-dir>/<workload>/<strategy>/{file-size.json,correctness.json,timing.json}
# and any *.parquet files produced for schema dumps.

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <results-dir>" >&2
  exit 1
fi

RESULTS="$1"

if [[ ! -d "$RESULTS" ]]; then
  echo "FAIL: $RESULTS not found" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "FAIL: jq required for scoring" >&2
  exit 1
fi

OUT_CSV="${RESULTS}/scoreboard.csv"
OUT_PERLOP="${RESULTS}/scoreboard-per-op.csv"
OUT_PARETO="${RESULTS}/pareto.txt"
OUT_SCHEMAS="${RESULTS}/schemas.txt"

echo "strategy,workload,write_ms_p50,write_ms_p95,read_ms_p50,read_ms_p95,file_bytes,file_count,rpr,afd,disqualified" > "$OUT_CSV"
echo "strategy,workload,operation,wall_ms_p50,wall_ms_p95,executor_cpu_ms_p50,executor_run_ms_p50,jvm_gc_ms_p50,records_read,bytes_read,shuffle_bytes_read,shuffle_bytes_written,peak_exec_mem_bytes,records_written,bytes_written" > "$OUT_PERLOP"

read_metric() {
  local file="$1"
  local query="$2"
  if [[ -f "$file" ]]; then
    jq -r "$query // 0" "$file" 2>/dev/null || echo 0
  else
    echo 0
  fi
}

# Compute the percentile of wall_clock_ms across iterations in one timing.json.
# Driver writes [{operation, iteration, wall_clock_ns, wall_clock_ms}, ...] per op.
percentile() {
  local file="$1"
  local pct="$2"
  if [[ ! -f "$file" ]]; then echo 0; return; fi
  jq --argjson p "$pct" '
    [.[] | .wall_clock_ms] | sort
    | if length == 0 then 0
      else .[((length - 1) * $p / 100) | floor]
      end
  ' "$file" 2>/dev/null || echo 0
}

# Sum the percentile across all per-op timing.json files matching a prefix.
# Driver layout: <cell>/<op>/parquet-<compression>/timing.json
sum_op_percentile() {
  local cell_dir="$1"
  local prefix="$2"
  local pct="$3"
  local total=0
  shopt -s nullglob
  for f in "${cell_dir}"${prefix}*/parquet-*/timing.json; do
    local v
    v=$(percentile "$f" "$pct")
    total=$((total + ${v:-0}))
  done
  shopt -u nullglob
  echo "$total"
}

# RPR (Row Preservation Ratio): for each shredded read op, compare row_count
# against the unshredded baseline cell for the same workload. Catches B1's
# silent-drop on mixed-type fields (predicate pushdown returns fewer rows than
# reality because the typed_value column never saw the dropped values).
#
# Strategy mapping: shredded read-filter-shredded vs unshredded read-filter-variant.
# RPR = min over ops of shredded.row_count / unshredded.row_count. 1.0 = perfect.
#
# AFD (Aggregate Fidelity Delta): same join, on sum_value. AFD = max over ops of
# |shredded.sum - unshredded.sum| / max(1, |unshredded.sum|). 0.0 = perfect.
#
# When the unshredded baseline cell does not exist for the workload, we cannot
# compute RPR/AFD; emit 1.0 / 0.0 with a comment so the row is not flagged as a
# disqualification, but the operator can see the gap.
compute_rpr_afd() {
  local cell_dir="$1"
  local workload="$2"
  local strategy="$3"
  local baseline_dir="${RESULTS}/${workload}/unshredded/correctness.json"
  local cell_correctness="${cell_dir}correctness.json"

  if [[ ! -f "$cell_correctness" ]] || [[ ! -f "$baseline_dir" ]]; then
    echo "1.0 0.0"
    return
  fi
  if [[ "$strategy" == "unshredded" ]]; then
    echo "1.0 0.0"
    return
  fi

  jq -r --slurpfile base "$baseline_dir" '
    def shredded_op_to_baseline:
      sub("-shredded$"; "-variant");
    def safe_div(a; b):
      if (b | tonumber) == 0 then 1.0 else (a | tonumber) / (b | tonumber) end;

    [to_entries[] | select(.key | test("read-filter-shredded|read-agg-shredded|sf-q[1-5]-shredded"))]
    | if length == 0 then "1.0 0.0"
      else
        ([ .[] |
           . as $cell |
           ($cell.key | shredded_op_to_baseline) as $bk |
           ($base[0][$bk] // null) as $b |
           if $b == null then null
           else
             {
               rpr: safe_div($cell.value.row_count; $b.row_count),
               afd: (if ($b.sum_value | tonumber) == 0
                     then 0.0
                     else (((($cell.value.sum_value | tonumber) - ($b.sum_value | tonumber)) | fabs) / (($b.sum_value | tonumber) | fabs))
                     end)
             }
           end
         ] | map(select(. != null))) as $pairs
        | if ($pairs | length) == 0 then "1.0 0.0"
          else
            ([$pairs[].rpr] | min | tostring) + " " + ([$pairs[].afd] | max | tostring)
          end
      end
  ' "$cell_correctness" 2>/dev/null || echo "1.0 0.0"
}

# Iterate cells: <results>/<workload>/<strategy>/...
shopt -s nullglob
for workload_dir in "${RESULTS}"/*/; do
  workload=$(basename "$workload_dir")
  # Skip non-workload dirs (jmh subdirs etc. would not be siblings here, but defensive).
  [[ "$workload" == "jmh"* ]] && continue
  [[ "$workload" == "summary"* ]] && continue

  for strategy_dir in "${workload_dir}"*/; do
    strategy=$(basename "$strategy_dir")

    file_size=$(read_metric "${strategy_dir}file-size.json" '.file_bytes')
    file_count=$(read_metric "${strategy_dir}file-size.json" '.file_count')

    write_p50=$(sum_op_percentile "$strategy_dir" "write-" 50)
    write_p95=$(sum_op_percentile "$strategy_dir" "write-" 95)
    read_p50=$(sum_op_percentile "$strategy_dir" "read-" 50)
    read_p95=$(sum_op_percentile "$strategy_dir" "read-" 95)

    rpr_afd=$(compute_rpr_afd "$strategy_dir" "$workload" "$strategy")
    rpr=$(echo "$rpr_afd" | awk '{print $1}')
    afd=$(echo "$rpr_afd" | awk '{print $2}')

    # Disqualified if RPR < 1.0 (correctness violation - rows dropped) or AFD > 0
    # (correctness violation - aggregate value diverges from baseline).
    disqualified=false
    if awk -v r="$rpr" 'BEGIN { exit !(r+0 < 1.0) }'; then
      disqualified=true
    elif awk -v a="$afd" 'BEGIN { exit !(a+0 > 0) }'; then
      disqualified=true
    fi

    echo "${strategy},${workload},${write_p50},${write_p95},${read_p50},${read_p95},${file_size},${file_count},${rpr},${afd},${disqualified}" >> "$OUT_CSV"

    # Per-op breakdown: one row per (strategy, workload, op) combining wall_clock
    # (timing.json) and Spark engine metrics (spark-metrics.json) so slides can cite
    # individual ops the way Snowflake's blog reports Q1-Q11 separately.
    shopt -s nullglob
    for op_dir in "${strategy_dir}"*/parquet-*/; do
      op=$(basename "$(dirname "$op_dir")")
      timing="${op_dir}timing.json"
      metrics="${op_dir}spark-metrics.json"
      [[ ! -f "$timing" ]] && continue
      wall_p50=$(percentile "$timing" 50)
      wall_p95=$(percentile "$timing" 95)
      if [[ -f "$metrics" ]]; then
        cpu_p50=$(jq -r '.summary.p50_executor_cpu_time_ms // 0' "$metrics" 2>/dev/null)
        run_p50=$(jq -r '.summary.p50_executor_run_time_ms // 0' "$metrics" 2>/dev/null)
        gc_p50=$(jq -r '.summary.p50_jvm_gc_time_ms // 0' "$metrics" 2>/dev/null)
        rec_read=$(jq -r '.summary.records_read_total // 0' "$metrics" 2>/dev/null)
        b_read=$(jq -r '.summary.bytes_read_total // 0' "$metrics" 2>/dev/null)
        sh_read=$(jq -r '.summary.shuffle_bytes_read_total // 0' "$metrics" 2>/dev/null)
        sh_write=$(jq -r '.summary.shuffle_bytes_written_total // 0' "$metrics" 2>/dev/null)
        peak=$(jq -r '.summary.peak_execution_memory_bytes_max // 0' "$metrics" 2>/dev/null)
        rec_wr=$(jq -r '.summary.records_written_total // 0' "$metrics" 2>/dev/null)
        b_wr=$(jq -r '.summary.bytes_written_total // 0' "$metrics" 2>/dev/null)
      else
        cpu_p50=0; run_p50=0; gc_p50=0; rec_read=0; b_read=0
        sh_read=0; sh_write=0; peak=0; rec_wr=0; b_wr=0
      fi
      echo "${strategy},${workload},${op},${wall_p50},${wall_p95},${cpu_p50},${run_p50},${gc_p50},${rec_read},${b_read},${sh_read},${sh_write},${peak},${rec_wr},${b_wr}" >> "$OUT_PERLOP"
    done
    shopt -u nullglob
  done
done
shopt -u nullglob

# Pareto table: sort by disqualified DESC (correctness-passing first), file_size ASC, read_p95 ASC.
{
  echo "Pareto frontier (correctness first, then size, then read latency)"
  echo ""
  printf "%-15s %-22s %12s %12s %12s %12s %12s %8s %8s %-12s\n" \
    "STRATEGY" "WORKLOAD" "WRITE_P50" "WRITE_P95" "READ_P50" "READ_P95" "FILE_BYTES" "RPR" "AFD" "DISQUAL"
  tail -n +2 "$OUT_CSV" \
    | sort -t, -k11,11 -k7,7n -k6,6n \
    | awk -F, '{printf "%-15s %-22s %12s %12s %12s %12s %12s %8s %8s %-12s\n", $1,$2,$3,$4,$5,$6,$7,$9,$10,$11}'
} > "$OUT_PARETO"

# Schema + row-group dump per cell. Reads from the warehouse (cell results dirs
# only hold timing/correctness JSON, not parquet). Prefers `parquet meta` (full
# schema + row groups + per-column stats + compression sizes); falls back to
# `parquet-tools schema` if only that is on PATH.
: > "$OUT_SCHEMAS"
WAREHOUSE_BASE="${WAREHOUSE_BASE:-/tmp/iceberg-bench/warehouse-shred-v2}"
dump_cmd=()
if command -v parquet >/dev/null 2>&1; then
  dump_cmd=(parquet meta)
elif command -v parquet-tools >/dev/null 2>&1; then
  dump_cmd=(parquet-tools schema)
else
  echo "(neither 'parquet' nor 'parquet-tools' on PATH; skipping schema dumps)" > "$OUT_SCHEMAS"
fi

if [[ ${#dump_cmd[@]} -gt 0 ]]; then
  shopt -s nullglob
  for workload_dir in "${RESULTS}"/*/; do
    workload=$(basename "$workload_dir")
    [[ "$workload" == "jmh"* ]] && continue
    [[ "$workload" == "summary"* ]] && continue
    for strategy_dir in "${workload_dir}"*/; do
      strategy=$(basename "$strategy_dir")
      # Try the matrix layout first (one warehouse dir per cell, named
      # <workload>-<strategy>). Fall back to a generic search rooted at
      # WAREHOUSE_BASE for smoke / single-cell runs where the driver writes
      # straight into WAREHOUSE_BASE/db/<table>/data/.
      warehouse_cell="${WAREHOUSE_BASE}/${workload}-${strategy}"
      # Target the canonical EVENTS table for this strategy so the schema dump
      # reflects what shredding inferred (or didn't). Without this, find returns
      # the alphabetically-first parquet (arrays_json/data/...) which is the
      # wrong table for every strategy comparison.
      #   unshredded -> events_variant (no typed_value subtree by design)
      #   anything else -> events_shredded (the shredded layout we want to inspect)
      if [[ "$strategy" == "unshredded" ]]; then
        events_table="events_variant"
      else
        events_table="events_shredded"
      fi
      # set -o pipefail + `find | head -1` causes find's SIGPIPE (141) to fail
      # the script. Use `find -print -quit` instead - stops at first match cleanly.
      pq=$(find "${warehouse_cell}/db/${events_table}/data" -name '*.parquet' -print -quit 2>/dev/null || true)
      if [[ -z "$pq" ]]; then
        # Per-table fallback: scan the entire cell warehouse.
        pq=$(find "$warehouse_cell" -name '*.parquet' -print -quit 2>/dev/null || true)
      fi
      if [[ -z "$pq" ]]; then
        # Global fallback: smoke / single-cell runs where driver writes straight
        # into WAREHOUSE_BASE/db/<table>/data/.
        pq=$(find "$WAREHOUSE_BASE" -name '*.parquet' -print -quit 2>/dev/null || true)
      fi
      if [[ -z "$pq" ]]; then
        continue
      fi
      echo "===== ${workload} / ${strategy}  (${pq}) =====" >> "$OUT_SCHEMAS"
      "${dump_cmd[@]}" "$pq" >> "$OUT_SCHEMAS" 2>&1 || true
      echo "" >> "$OUT_SCHEMAS"
    done
  done
  shopt -u nullglob
fi

echo "Wrote $OUT_CSV"
echo "Wrote $OUT_PERLOP"
echo "Wrote $OUT_PARETO"
echo "Wrote $OUT_SCHEMAS"

# Per-column shred scoreboard - two sources:
# 1. Footer-based scorer (authoritative, includes nested shreds, requires .venv/pyarrow)
# 2. Quick-check aggregation (fallback if footer scorer unavailable, only counts top-level primitive shreds)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Determine the warehouse base for THIS results run. Prefer whatever the driver wrote to
# during the matrix cells - typically /tmp/iceberg-bench/warehouse-shred-v2.
WAREHOUSE_BASE="${WAREHOUSE_BASE:-/tmp/iceberg-bench/warehouse-shred-v2}"

if [ -x "$REPO_ROOT/.venv/bin/python" ] && [ -d "$WAREHOUSE_BASE" ]; then
  "$SCRIPT_DIR/score-per-column.sh" \
    --warehouse-base "$WAREHOUSE_BASE" \
    --output "$RESULTS/scoreboard-columns.csv" \
    || echo "(footer-based per-column scoring failed)"
else
  echo "(skipping footer-based per-column scoring: missing .venv or warehouse at $WAREHOUSE_BASE)"
fi

# Quick-check-based aggregation (only useful if you ran the quick-check separately).
# Writes to scoreboard-quickcheck.csv - deliberately different filename from
# scoreboard-columns.csv (which the footer scorer above owns) so they never collide.
SCORES_GLOB="$REPO_ROOT/benchmark/results/quickcheck-*-scores.csv"
if compgen -G "$SCORES_GLOB" > /dev/null 2>&1; then
  python3 "$SCRIPT_DIR/aggregate-column-scores.py" "$RESULTS" "$SCORES_GLOB" > "$RESULTS/scoreboard-quickcheck.csv.log" 2>&1 || true
fi
