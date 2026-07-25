#!/usr/bin/env bash
# Pre-builds all workload staging directories so the matrix run is just cells.
# Idempotent: skips any workload whose staging already has parquet files.
#
# Staging layout is scale-scoped: each scale gets its own subdir so multiple
# scales can coexist:
#   /tmp/iceberg-bench/staging-shred-v2/nano/w1-uniform/events/...
#   /tmp/iceberg-bench/staging-shred-v2/small/w1-uniform/events/...
#
# Usage:
#   ./benchmark/scripts/build-workloads.sh                    # all workloads, scale=small
#   BENCH_SCALE=nano ./benchmark/scripts/build-workloads.sh   # 100k-row staging
#   BENCH_SCALE=micro ./benchmark/scripts/build-workloads.sh  # 1M-row staging
#   BENCH_SCALE=tiny ./benchmark/scripts/build-workloads.sh   # 1000-row staging (smoke)
#   ./benchmark/scripts/build-workloads.sh w1-uniform w4-long-array   # specific workloads
#
# w5-clustered is special: real GitHub Archive data fetched ONCE to a durable
# location and symlinked into each scale dir. See benchmark/scripts/fetch-w5-clustered.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCALE="${BENCH_SCALE:-small}"
STAGING_BASE="/tmp/iceberg-bench/staging-shred-v2/${SCALE}"

DEFAULT_WORKLOADS=(w1-uniform w2-uuid w3-mixed-60-40 w3-mixed-95-5 w3-mixed-99-1 w4-long-array w5-clustered w8-wide-object w9-blob-payload w10-polymorphic w11-deeply-nested)

if [[ $# -gt 0 ]]; then
  WORKLOADS=("$@")
else
  WORKLOADS=("${DEFAULT_WORKLOADS[@]}")
fi

mkdir -p "$STAGING_BASE"

echo "===== Pre-building workload staging ====="
echo " Workloads: ${WORKLOADS[*]}"
echo " Scale:     ${SCALE}"
echo " Base:      ${STAGING_BASE}"
echo ""

start=$(date +%s)
for workload in "${WORKLOADS[@]}"; do
  staging="${STAGING_BASE}/${workload}"

  if [[ -d "${staging}/events" ]] \
     && [[ -n "$(find "${staging}/events" -name '*.parquet' -print -quit 2>/dev/null)" ]]; then
    count=$(find "${staging}/events" -name '*.parquet' | wc -l | tr -d ' ')
    echo ">>> SKIP ${workload}: ${count} parquet files already present at ${staging}/events"
    continue
  fi

  echo ""
  echo ">>> BUILD ${workload} at scale=${SCALE}"
  if [[ "$workload" == "w5-clustered" ]]; then
    # w5 is real-world data (GitHub Archive 2024-01-01, ~11.5M rows across 258 files).
    # Fetch ONCE to durable location, then materialize per scale by copying enough
    # source parquet files to approximate the scale's row target. Otherwise every
    # scale reads the full 1.2 GB corpus and dominates sweep wall-clock.
    durable="${HOME}/iceberg-bench-data/staging-shred-v2/w5-clustered"
    if [[ ! -d "${durable}/events" ]] \
       || [[ -z "$(find "${durable}/events" -name '*.parquet' -print -quit 2>/dev/null)" ]]; then
      echo "FAIL: w5-clustered durable data not present at ${durable}/events." >&2
      echo "Run: ./benchmark/scripts/fetch-w5-clustered.sh" >&2
      echo "Then re-run this script." >&2
      exit 1
    fi
    # Row target per scale (matches DataGenerator scale constants).
    case "$SCALE" in
      tiny)   target_rows=1000 ;;
      nano)   target_rows=100000 ;;
      micro)  target_rows=1000000 ;;
      small)  target_rows=10000000 ;;
      medium) target_rows=100000000 ;;
      *)      target_rows=0 ;;  # 0 = use full corpus (no cap)
    esac
    mkdir -p "${staging}/events"
    if [[ "$target_rows" -eq 0 ]]; then
      echo "    linking full corpus (scale=$SCALE has no row cap)"
      rm -rf "${staging}"
      ln -s "${durable}" "${staging}"
    else
      # Iterate durable files in sorted order, symlink until row target met.
      # Each source file is ~40-80k rows; we always link at least one file so
      # tiny still has data even though 1000 < smallest file's row count.
      rm -rf "${staging}/events"
      mkdir -p "${staging}/events"
      accumulated=0
      linked=0
      for src in $(find "${durable}/events" -maxdepth 1 -name '*.parquet' | sort); do
        n=$("${REPO_ROOT:-$(cd "${SCRIPT_DIR}/../.." && pwd)}/.venv/bin/python" \
              -c "import pyarrow.parquet as pq,sys; print(pq.ParquetFile(sys.argv[1]).metadata.num_rows)" \
              "$src" 2>/dev/null || echo 0)
        ln -sf "$src" "${staging}/events/$(basename "$src")"
        accumulated=$((accumulated + n))
        linked=$((linked + 1))
        if [[ "$accumulated" -ge "$target_rows" ]]; then
          break
        fi
      done
      echo "    linked $linked file(s), ~$accumulated rows (target: $target_rows)"
    fi
    continue
  fi
  BENCH_SCALE="${SCALE}" bash "${SCRIPT_DIR}/ensure-workload.sh" "$workload" "$staging"
done

end=$(date +%s)
elapsed=$((end - start))

echo ""
echo "===== Pre-build complete in ${elapsed}s ====="
for workload in "${WORKLOADS[@]}"; do
  staging="${STAGING_BASE}/${workload}"
  if [[ -d "${staging}/events" ]]; then
    count=$(find "${staging}/events" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
    bytes=$(find "${staging}/events" -name '*.parquet' -exec stat -f%z {} \; 2>/dev/null | awk '{sum+=$1} END {print sum+0}')
    printf "  %-20s %4d parquet files  %12d bytes\n" "$workload" "$count" "$bytes"
  else
    printf "  %-20s MISSING\n" "$workload"
  fi
done
