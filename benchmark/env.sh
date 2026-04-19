#!/usr/bin/env bash
# Iceberg Baseline Benchmark - Environment Configuration
# Pin all versions and paths for reproducibility.

set -euo pipefail

# --- Version Pins ---
export JAVA_VERSION=17
export ICEBERG_GIT_SHA="${ICEBERG_GIT_SHA:-HEAD}"
export SPARK_VERSION=4.1
export FLINK_VERSION=2.1
export SCALA_VERSION=2.13
export ASYNC_PROFILER_VERSION=3.0

# --- Parallelism ---
# Both engines pinned to same thread count for controlled comparison.
export BENCH_THREADS=4

# --- Paths ---
export BENCH_HOME="${BENCH_HOME:-/tmp/iceberg-bench}"
export BENCH_WAREHOUSE="${BENCH_HOME}/warehouse"
export BENCH_STAGING="${BENCH_HOME}/staging"
export BENCH_RESULTS="${BENCH_HOME}/results"

# Iceberg repo root (auto-detect from this script's location)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export ICEBERG_HOME="${SCRIPT_DIR}/.."

# async-profiler (user must set this if profiling is enabled)
export ASYNC_PROFILER_HOME="${ASYNC_PROFILER_HOME:-}"

# --- JVM Configuration ---
export BENCH_JVM_OPTS="-Xmx16g -XX:+UseG1GC"

# --- Scale Defaults ---
# small=10M, medium=100M, large=500M rows for events table
export BENCH_SCALE="${BENCH_SCALE:-medium}"

# --- Create directories ---
init_dirs() {
  mkdir -p "${BENCH_WAREHOUSE}/spark" "${BENCH_WAREHOUSE}/flink" \
           "${BENCH_STAGING}" "${BENCH_RESULTS}"
  echo "Benchmark directories initialized at ${BENCH_HOME}"
}

# --- Classpath Assembly ---
# Assembles classpath from Gradle build output for a given engine.
spark_classpath() {
  local spark_jars
  spark_jars=$(find "${ICEBERG_HOME}/spark/v${SPARK_VERSION}/spark/build/libs" \
    "${ICEBERG_HOME}/spark/v${SPARK_VERSION}/spark-extensions/build/libs" \
    "${ICEBERG_HOME}/spark/v${SPARK_VERSION}/spark-runtime/build/libs" \
    "${ICEBERG_HOME}/core/build/libs" \
    "${ICEBERG_HOME}/api/build/libs" \
    "${ICEBERG_HOME}/bundled-guava/build/libs" \
    "${ICEBERG_HOME}/parquet/build/libs" \
    "${ICEBERG_HOME}/data/build/libs" \
    "${ICEBERG_HOME}/common/build/libs" \
    -name "*.jar" 2>/dev/null | tr '\n' ':')
  echo "${spark_jars}"
}

flink_classpath() {
  local flink_jars
  flink_jars=$(find "${ICEBERG_HOME}/flink/v${FLINK_VERSION}/flink/build/libs" \
    "${ICEBERG_HOME}/flink/v${FLINK_VERSION}/flink-runtime/build/libs" \
    "${ICEBERG_HOME}/core/build/libs" \
    "${ICEBERG_HOME}/api/build/libs" \
    "${ICEBERG_HOME}/bundled-guava/build/libs" \
    "${ICEBERG_HOME}/parquet/build/libs" \
    "${ICEBERG_HOME}/data/build/libs" \
    "${ICEBERG_HOME}/common/build/libs" \
    -name "*.jar" 2>/dev/null | tr '\n' ':')
  echo "${flink_jars}"
}

# --- Hardware Info ---
print_hardware_info() {
  echo "=== Hardware Info ==="
  echo "OS: $(uname -s)"
  echo "Arch: $(uname -m)"
  if command -v sysctl &>/dev/null; then
    sysctl -n machdep.cpu.brand_string 2>/dev/null || true
    echo "Cores: $(sysctl -n hw.ncpu 2>/dev/null || nproc)"
    echo "RAM: $(sysctl -n hw.memsize 2>/dev/null | awk '{print $1/1024/1024/1024 " GB"}' || free -h | awk '/Mem:/{print $2}')"
  elif [ -f /proc/cpuinfo ]; then
    grep "model name" /proc/cpuinfo | head -1
    echo "Cores: $(nproc)"
    free -h | awk '/Mem:/{print "RAM: " $2}'
  fi
  echo "===================="
}

# --- Cache Drop ---
drop_caches() {
  if [[ "$(uname)" == "Darwin" ]]; then
    purge 2>/dev/null || echo "Warning: 'purge' requires sudo on macOS"
  elif [[ "$(uname)" == "Linux" ]]; then
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null 2>&1 || \
      echo "Warning: cache drop requires root"
  fi
}

# Source this file: source benchmark/env.sh
