#!/usr/bin/env bash
# Sensitivity sweep: vary write.parquet.variant-inference-buffer-size on shredded variant.
# Buffer sizes: 10, 100 (default; tracked as the existing events_shredded table), 1000, 10000.
#
# This script intentionally re-runs write-shredded (buf=100 baseline) so the buf=100 row in
# the sweep tables is measured under the same conditions as buf-{10,1000,10000}. It uses the
# same warehouse-v2-${scale} path as run_spark.sh so tables from a prior baseline run are
# reusable, and uses two gradle invocations (writes then reads) so the read measurement
# protocol matches run_spark.sh (1 warmup + 3 iters for writes; 2 warmup + 5 iters for reads,
# OS page cache dropped between).
#
# Usage:
#   ./benchmark/scripts/sensitivity.sh           # small only (default; medium adds ~50 min)
#   ./benchmark/scripts/sensitivity.sh small

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="${SCRIPT_DIR}/.."
REPO_ROOT="${BENCH_DIR}/.."
SCALE="${1:-small}"

if [[ "$SCALE" != "small" ]]; then
  echo "ERROR: sensitivity sweep is supported at small scale only (got: $SCALE)." >&2
  echo "       Medium would add ~50 min and the per-buffer trend has the same shape." >&2
  exit 1
fi

STAGING="/tmp/iceberg-bench/staging-v2-${SCALE}"
WAREHOUSE="/tmp/iceberg-bench/warehouse-v2-${SCALE}"

if [[ ! -d "${STAGING}/events" ]]; then
  echo "ERROR: No staging data at ${STAGING}." >&2
  echo "       Generate it first: ./benchmark/scripts/run_spark.sh ${SCALE}" >&2
  exit 1
fi

mkdir -p "${WAREHOUSE}"

WRITE_OPS="write-shredded,write-shredded-buf10,write-shredded-buf1000,write-shredded-buf10000"
READ_OPS="read-project-shredded,read-project-shredded-buf10,read-project-shredded-buf1000,read-project-shredded-buf10000"

GRADLE_COMMON="-DsparkVersions=4.1 -DflinkVersions= -DscalaVersion=2.13"

# --- Writes: populate events_shredded + events_shredded_buf{10,1000,10000} ---
echo ""
echo "===== Sensitivity writes - ${SCALE} ====="
WRITE_RESULTS="${BENCH_DIR}/runs/$(date +%Y%m%d_%H%M%S)_spark_sensitivity_writes_${SCALE}"
mkdir -p "${WRITE_RESULTS}"
echo '{"engine":"spark","kind":"sensitivity-writes"}' > "${WRITE_RESULTS}/run-metadata.json"

cd "$REPO_ROOT"
./gradlew :iceberg-benchmark:sparkBench ${GRADLE_COMMON} \
  -Dbench.warehouse="${WAREHOUSE}" \
  -Dbench.staging="${STAGING}" \
  -Dbench.results="${WRITE_RESULTS}" \
  -Dbench.threads=4 \
  -Dbench.warmup=1 \
  -Dbench.iterations=3 \
  -Dbench.compression=zstd \
  -Dbench.operations="${WRITE_OPS}" \
  2>&1 | tee "${WRITE_RESULTS}/benchmark.log"

# --- Drop OS page cache between writes and reads ---
if [[ "$(uname)" == "Darwin" ]]; then
  purge 2>/dev/null || true
elif [[ "$(uname)" == "Linux" ]]; then
  sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null 2>&1 || true
fi

# --- Reads: same warehouse, tables already populated ---
echo ""
echo "===== Sensitivity reads - ${SCALE} ====="
READ_RESULTS="${BENCH_DIR}/runs/$(date +%Y%m%d_%H%M%S)_spark_sensitivity_reads_${SCALE}"
mkdir -p "${READ_RESULTS}"
echo '{"engine":"spark","kind":"sensitivity-reads"}' > "${READ_RESULTS}/run-metadata.json"

cd "$REPO_ROOT"
./gradlew :iceberg-benchmark:sparkBench ${GRADLE_COMMON} \
  -Dbench.warehouse="${WAREHOUSE}" \
  -Dbench.staging="${STAGING}" \
  -Dbench.results="${READ_RESULTS}" \
  -Dbench.threads=4 \
  -Dbench.warmup=2 \
  -Dbench.iterations=5 \
  -Dbench.compression=zstd \
  -Dbench.operations="${READ_OPS}" \
  2>&1 | tee "${READ_RESULTS}/benchmark.log"

# --- Collect summaries ---
echo ""
bash "${BENCH_DIR}/results/collect-metrics.sh" "${WRITE_RESULTS}"
bash "${BENCH_DIR}/results/collect-metrics.sh" "${READ_RESULTS}"

if command -v python3 &>/dev/null; then
  python3 "${BENCH_DIR}/results/report.py" --input "${READ_RESULTS}" --output "${READ_RESULTS}/summary"
fi

echo ""
echo "===== Sensitivity sweep complete - $(date) ====="
echo "Writes: ${WRITE_RESULTS}"
echo "Reads:  ${READ_RESULTS}"
