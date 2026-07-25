#!/usr/bin/env bash
# Maps a workload ID to its staging-directory build steps.
# Idempotent: skips if staging/events already exists.
#
# Usage:
#   ./benchmark/scripts/ensure-workload.sh <workload-id> <staging-dir>
#
# Workload IDs:
#   w1-uniform        -> base DataGenerator output (no augmenter)
#   w2-uuid           -> high_card augmenter
#   w3-mixed-60-40    -> mixed_type augmenter, ratio 0.60
#   w3-mixed-95-5     -> mixed_type augmenter, ratio 0.95
#   w3-mixed-99-1     -> mixed_type augmenter, ratio 0.99
#   w4-long-array     -> long_array augmenter
#   w5-clustered      -> GitHub Archive 2024-01-01 (sorted by event type)
#   w6-drift          -> drift augmenter (conditional; only if explicitly enabled)
#   w7-boundary       -> unit-test only; no staging produced (succeeds with note)
#   w8-wide-object    -> wide_object augmenter (Phase 3.5)
#   w9-blob-payload   -> blob_payload augmenter (Phase 3.5)
#   w10-polymorphic   -> polymorphic augmenter (Phase 3.5)
#   w11-deeply-nested -> deeply_nested augmenter (Phase 3.5)

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <workload-id> <staging-dir>" >&2
  exit 1
fi

WORKLOAD="$1"
STAGING="$2"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${BENCH_DIR}/.." && pwd)"
AUG="${SCRIPT_DIR}/augmenters/run.sh"

if [[ -d "${STAGING}/events" ]] \
   && [[ -n "$(find "${STAGING}/events" -name '*.parquet' -print -quit 2>/dev/null)" ]]; then
  echo "Staging already present at ${STAGING}/events - skipping."
  exit 0
fi

# Wipe any leftover empty events dir from a prior failed run so the augmenter
# can write fresh.
rm -rf "${STAGING}/events"

mkdir -p "$STAGING"

generate_base() {
  local target="$1"
  local scale="${BENCH_SCALE:-small}"
  echo ">>> generating base data into ${target} (scale=${scale})..."
  cd "$REPO_ROOT"
  ./gradlew :iceberg-benchmark:generateData \
    -DsparkVersions=4.1 -DflinkVersions= -DscalaVersion=2.13 \
    -Dbench.scale="${scale}" \
    -Dbench.staging="${target}" \
    --parallel --max-workers=4
}

run_aug() {
  local name="$1"
  local in_dir="$2"
  local out_dir="$3"
  shift 3
  mkdir -p "${out_dir}/events"
  local count=0
  for f in "${in_dir}/events"/*.parquet; do
    [[ -f "$f" ]] || continue
    bash "$AUG" "$name" "$f" "${out_dir}/events/$(basename "$f")" "$@" \
      || { echo "FAIL: augmenter ${name} failed on ${f}" >&2; exit 1; }
    count=$((count + 1))
  done
  if [[ $count -eq 0 ]]; then
    echo "FAIL: no .parquet files found under ${in_dir}/events" >&2
    exit 1
  fi
  local written
  written=$(find "${out_dir}/events" -name '*.parquet' | wc -l | tr -d ' ')
  if [[ $written -eq 0 ]]; then
    echo "FAIL: augmenter ${name} processed ${count} input files but produced 0 output files" >&2
    exit 1
  fi
  echo ">>> augmenter ${name}: ${count} input files -> ${written} output files"
}

case "$WORKLOAD" in
  w1-uniform)
    generate_base "$STAGING"
    ;;
  w2-uuid)
    base="${STAGING}-base"
    generate_base "$base"
    run_aug high_card "$base" "$STAGING"
    ;;
  w3-mixed-60-40)
    base="${STAGING}-base"
    generate_base "$base"
    INT_RATIO=0.60 run_aug mixed_type "$base" "$STAGING"
    ;;
  w3-mixed-95-5)
    base="${STAGING}-base"
    generate_base "$base"
    INT_RATIO=0.95 run_aug mixed_type "$base" "$STAGING"
    ;;
  w3-mixed-99-1)
    base="${STAGING}-base"
    generate_base "$base"
    INT_RATIO=0.99 run_aug mixed_type "$base" "$STAGING"
    ;;
  w4-long-array)
    base="${STAGING}-base"
    generate_base "$base"
    run_aug long_array "$base" "$STAGING"
    ;;
  w5-clustered)
    bash "${SCRIPT_DIR}/gh_archive_load.sh" 2024-01-01 "${STAGING}/events"
    ;;
  w6-drift)
    if [[ "${ENABLE_W6:-0}" != "1" ]]; then
      echo "W6 (drift) is conditional. Set ENABLE_W6=1 to build it." >&2
      exit 1
    fi
    base="${STAGING}-base"
    generate_base "$base"
    run_aug drift "$base" "$STAGING"
    ;;
  w7-boundary)
    echo "W7 (boundary) is a unit-test-only workload. No staging produced."
    echo "Run: ./gradlew :iceberg-parquet:test --tests 'TestInferenceStrategies' -Pquick=true -x javadoc"
    mkdir -p "${STAGING}/events"
    touch "${STAGING}/events/.unit-test-only"
    ;;
  w8-wide-object)
    base="${STAGING}-base"
    generate_base "$base"
    run_aug wide_object "$base" "$STAGING"
    ;;
  w9-blob-payload)
    base="${STAGING}-base"
    generate_base "$base"
    run_aug blob_payload "$base" "$STAGING"
    ;;
  w10-polymorphic)
    base="${STAGING}-base"
    generate_base "$base"
    run_aug polymorphic "$base" "$STAGING"
    ;;
  w11-deeply-nested)
    base="${STAGING}-base"
    generate_base "$base"
    run_aug deeply_nested "$base" "$STAGING"
    ;;
  *)
    echo "Unknown workload: $WORKLOAD" >&2
    echo "Valid IDs: w1-uniform w2-uuid w3-mixed-60-40 w3-mixed-95-5 w3-mixed-99-1 w4-long-array w5-clustered w6-drift w7-boundary w8-wide-object w9-blob-payload w10-polymorphic w11-deeply-nested" >&2
    exit 1
    ;;
esac

echo "===== Workload ready: $WORKLOAD ====="
echo " Staging: ${STAGING}/events"
