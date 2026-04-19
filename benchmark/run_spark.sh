#!/usr/bin/env bash
# Spark benchmarks across all scales.
# Writes run BEFORE reads - writes populate the tables that reads query.
# Both share the same warehouse directory per scale.
#
# Usage:
#   ./benchmark/run_spark.sh          # all scales
#   ./benchmark/run_spark.sh small    # just small
#   ./benchmark/run_spark.sh medium   # just medium
#   ./benchmark/run_spark.sh large    # just large

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SCRIPT_DIR}/.."
FILTER="${1:-all}"

WRITE_OPS="write-json,write-variant"
READ_OPS="read-baseline,read-project-json,read-project-variant,read-nested-json,read-nested-variant,read-filter-json,read-filter-variant,read-agg-json,read-agg-variant"

GRADLE_COMMON="-DsparkVersions=4.1 -DflinkVersions= -DscalaVersion=2.13"

run_spark() {
  local scale=$1
  local staging="/tmp/iceberg-bench/staging-v2-${scale}"
  local warehouse="/tmp/iceberg-bench/warehouse-v2-${scale}"

  if [[ ! -d "${staging}/events" ]]; then
    echo "ERROR: No staging data at ${staging}. Generating..." >&2
    cd "$REPO_ROOT"
    ./gradlew :iceberg-benchmark:generateData ${GRADLE_COMMON} \
      -Dbench.scale="${scale}" \
      -Dbench.staging="${staging}"
  fi

  mkdir -p "${warehouse}"

  # Writes first - populate the tables
  echo ""
  echo "===== Spark writes - ${scale} ====="
  local write_results="${SCRIPT_DIR}/runs/$(date +%Y%m%d_%H%M%S)_spark_writes_${scale}"
  mkdir -p "${write_results}"
  echo '{"engine":"spark"}' > "${write_results}/run-metadata.json"

  cd "$REPO_ROOT"
  ./gradlew :iceberg-benchmark:sparkBench ${GRADLE_COMMON} \
    -Dbench.warehouse="${warehouse}" \
    -Dbench.staging="${staging}" \
    -Dbench.results="${write_results}" \
    -Dbench.threads=4 \
    -Dbench.warmup=1 \
    -Dbench.iterations=3 \
    -Dbench.compression=zstd \
    -Dbench.operations="${WRITE_OPS}" \
    2>&1 | tee "${write_results}/benchmark.log"

  # Reads second - same warehouse, tables already populated
  echo ""
  echo "===== Spark reads - ${scale} ====="
  local read_results="${SCRIPT_DIR}/runs/$(date +%Y%m%d_%H%M%S)_spark_reads_${scale}"
  mkdir -p "${read_results}"
  echo '{"engine":"spark"}' > "${read_results}/run-metadata.json"

  # Drop OS caches before reads
  if [[ "$(uname)" == "Darwin" ]]; then
    purge 2>/dev/null || true
  elif [[ "$(uname)" == "Linux" ]]; then
    sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null 2>&1 || true
  fi

  cd "$REPO_ROOT"
  ./gradlew :iceberg-benchmark:sparkBench ${GRADLE_COMMON} \
    -Dbench.warehouse="${warehouse}" \
    -Dbench.staging="${staging}" \
    -Dbench.results="${read_results}" \
    -Dbench.threads=4 \
    -Dbench.warmup=2 \
    -Dbench.iterations=5 \
    -Dbench.compression=zstd \
    -Dbench.operations="${READ_OPS}" \
    2>&1 | tee "${read_results}/benchmark.log"

  # Collect metrics
  echo ""
  bash "${SCRIPT_DIR}/results/collect-metrics.sh" "${write_results}"
  bash "${SCRIPT_DIR}/results/collect-metrics.sh" "${read_results}"

  if command -v python3 &>/dev/null; then
    python3 "${SCRIPT_DIR}/results/report.py" --input "${read_results}" --output "${read_results}/summary"
  fi
}

echo "===== Spark Benchmarks - $(date) ====="

for scale in small medium large; do
  if [[ "$FILTER" == "all" || "$FILTER" == "$scale" ]]; then
    run_spark "$scale"
  fi
done

echo ""
echo "===== Spark complete - $(date) ====="
echo "Results:"
ls -dt "${SCRIPT_DIR}"/runs/*spark* 2>/dev/null | head -20
