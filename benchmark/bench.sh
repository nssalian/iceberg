#!/usr/bin/env bash
# Iceberg Baseline Benchmark - Runner
#
# Each invocation creates a timestamped results directory so runs never overwrite each other.
#
# Usage:
#   ./benchmark/bench.sh                                  # Generate data + run all Spark benchmarks (small)
#   ./benchmark/bench.sh --engine spark --scale medium     # Spark only, medium scale
#   ./benchmark/bench.sh --engine flink-batch              # Flink batch only
#   ./benchmark/bench.sh --engine flink-stream             # Flink streaming only
#   ./benchmark/bench.sh --ops read-full-scan,read-projection  # Specific operations only
#   ./benchmark/bench.sh --skip-datagen                    # Reuse existing staged data
#   ./benchmark/bench.sh --format orc --compression snappy # Override format/compression
#   ./benchmark/bench.sh --tag "after-compaction-fix"      # Tag the run for easy identification

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SCRIPT_DIR}/.."

# Defaults
ENGINE="spark"
SCALE="small"
FORMAT="parquet"
COMPRESSION="zstd"
OPS="all"
WARMUP=2
ITERS=5
THREADS=4
SKIP_DATAGEN=false
TAG=""
STAGING="/tmp/iceberg-bench/staging"

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --engine)       ENGINE="$2"; shift 2;;
    --scale)        SCALE="$2"; shift 2;;
    --format)       FORMAT="$2"; shift 2;;
    --compression)  COMPRESSION="$2"; shift 2;;
    --ops)          OPS="$2"; shift 2;;
    --warmup)       WARMUP="$2"; shift 2;;
    --iterations)   ITERS="$2"; shift 2;;
    --threads)      THREADS="$2"; shift 2;;
    --skip-datagen) SKIP_DATAGEN=true; shift;;
    --tag)          TAG="$2"; shift 2;;
    --staging)      STAGING="$2"; shift 2;;
    --help|-h)
      sed -n '2,/^$/p' "$0" | sed 's/^# \?//'
      exit 0
      ;;
    *) echo "Unknown option: $1. Use --help for usage." >&2; exit 1;;
  esac
done

# Build timestamp and results directory
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RUN_NAME="${TIMESTAMP}_${ENGINE}_${FORMAT}_${SCALE}"
if [[ -n "$TAG" ]]; then
  RUN_NAME="${RUN_NAME}_${TAG}"
fi
RESULTS_DIR="${SCRIPT_DIR}/runs/${RUN_NAME}"
WAREHOUSE="/tmp/iceberg-bench/warehouse/${RUN_NAME}"

mkdir -p "$RESULTS_DIR" "$WAREHOUSE"

# Save run metadata
cat > "${RESULTS_DIR}/run-metadata.json" <<METADATA
{
  "timestamp": "${TIMESTAMP}",
  "engine": "${ENGINE}",
  "scale": "${SCALE}",
  "format": "${FORMAT}",
  "compression": "${COMPRESSION}",
  "operations": "${OPS}",
  "warmup": ${WARMUP},
  "iterations": ${ITERS},
  "threads": ${THREADS},
  "format_version": 3,
  "tag": "${TAG}",
  "staging": "${STAGING}",
  "warehouse": "${WAREHOUSE}",
  "results": "${RESULTS_DIR}",
  "iceberg_sha": "$(cd "$REPO_ROOT" && git rev-parse --short HEAD 2>/dev/null || echo 'unknown')",
  "java_version": "$(java -version 2>&1 | head -1 | sed 's/.*"\(.*\)".*/\1/')",
  "os": "$(uname -s)"
}
METADATA

echo "============================================="
echo " Iceberg Baseline Benchmark"
echo "============================================="
echo " Engine:      ${ENGINE}"
echo " Scale:       ${SCALE}"
echo " Format:      ${FORMAT} / ${COMPRESSION}"
echo " Operations:  ${OPS}"
echo " Warmup:      ${WARMUP}, Iterations: ${ITERS}"
echo " Threads:     ${THREADS}"
echo " Results:     ${RESULTS_DIR}"
echo " Warehouse:   ${WAREHOUSE}"
if [[ -n "$TAG" ]]; then
  echo " Tag:         ${TAG}"
fi
echo "============================================="
echo ""

# Only include engine versions needed - avoids building Flink when running Spark and vice versa
case "$ENGINE" in
  spark)
    GRADLE_COMMON="-DsparkVersions=4.1 -DflinkVersions= -DscalaVersion=2.13"
    ;;
  flink-batch|flink-stream)
    GRADLE_COMMON="-DsparkVersions= -DflinkVersions=1.20 -DscalaVersion=2.12"
    ;;
esac

# Step 1: Generate data if needed
if [[ "$SKIP_DATAGEN" == false ]]; then
  if [[ -d "${STAGING}/events" ]]; then
    echo "Staging data already exists at ${STAGING}"
    echo "  Use --skip-datagen to reuse, or --staging <path> for a different location"
    echo "  Reusing existing data."
    echo ""
  else
    echo ">>> Generating data (scale=${SCALE})..."
    echo ""
    cd "$REPO_ROOT"
    ./gradlew :iceberg-benchmark:generateData ${GRADLE_COMMON} \
      -Dbench.scale="${SCALE}" \
      -Dbench.staging="${STAGING}" \
      2>&1 | tee "${RESULTS_DIR}/datagen.log"
    echo ""
    echo "Data generation complete."
    echo ""
  fi
else
  echo "Skipping data generation (--skip-datagen)"
  echo ""
fi

# Step 2: Drop OS page caches before benchmark run
if [[ "$(uname)" == "Darwin" ]]; then
  purge 2>/dev/null && echo "OS page cache purged" || true
elif [[ "$(uname)" == "Linux" ]]; then
  sync
  echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null 2>&1 && echo "OS page cache dropped" || true
fi

# Step 3: Run the benchmark
GRADLE_TASK=""
case "$ENGINE" in
  spark)        GRADLE_TASK="sparkBench";;
  flink-batch)  GRADLE_TASK="flinkBatchBench";;
  flink-stream) GRADLE_TASK="flinkStreamBench";;
  *)
    echo "Unknown engine: ${ENGINE}. Use spark, flink-batch, or flink-stream." >&2
    exit 1
    ;;
esac

echo ">>> Running ${ENGINE} benchmark..."
echo ""

cd "$REPO_ROOT"
./gradlew ":iceberg-benchmark:${GRADLE_TASK}" ${GRADLE_COMMON} \
  -Dbench.warehouse="${WAREHOUSE}" \
  -Dbench.staging="${STAGING}" \
  -Dbench.results="${RESULTS_DIR}" \
  -Dbench.threads="${THREADS}" \
  -Dbench.warmup="${WARMUP}" \
  -Dbench.iterations="${ITERS}" \
  -Dbench.format="${FORMAT}" \
  -Dbench.compression="${COMPRESSION}" \
  -Dbench.operations="${OPS}" \
  2>&1 | tee "${RESULTS_DIR}/benchmark.log"

echo ""

# Step 3: Collect summary if timing files exist
TIMING_COUNT=$(find "${RESULTS_DIR}" -name "timing.json" 2>/dev/null | wc -l | tr -d ' ')
if [[ "$TIMING_COUNT" -gt 0 ]]; then
  echo ">>> Collecting metrics from ${TIMING_COUNT} timing files..."
  bash "${SCRIPT_DIR}/results/collect-metrics.sh" "${RESULTS_DIR}"

  if command -v python3 &>/dev/null; then
    echo ""
    echo ">>> Generating report..."
    python3 "${SCRIPT_DIR}/results/report.py" \
      --input "${RESULTS_DIR}" \
      --output "${RESULTS_DIR}/summary"
  fi
fi

echo ""
echo "============================================="
echo " Run complete: ${RUN_NAME}"
echo " Results:      ${RESULTS_DIR}"
if [[ -f "${RESULTS_DIR}/summary/summary_stats.csv" ]]; then
  echo " Summary:      ${RESULTS_DIR}/summary/summary_stats.csv"
fi
echo ""
echo " To list all runs:"
echo "   ls -lt /tmp/iceberg-bench/results/"
echo ""
echo " To compare two runs:"
echo "   diff <(cat run1/summary/summary_stats.csv) <(cat run2/summary/summary_stats.csv)"
echo "============================================="
