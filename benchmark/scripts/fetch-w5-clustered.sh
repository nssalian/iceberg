#!/usr/bin/env bash
# Downloads the w5-clustered GitHub Archive workload to a durable location.
# Idempotent: skips download if parquet files already exist there.
# Default destination: ~/iceberg-bench-data/staging-shred-v2/w5-clustered
#
# Usage:
#   ./benchmark/scripts/fetch-w5-clustered.sh                    # default dest
#   ./benchmark/scripts/fetch-w5-clustered.sh /custom/path       # override dest
#   FORCE=1 ./benchmark/scripts/fetch-w5-clustered.sh            # re-download even if present

set -euo pipefail

DEFAULT_DEST="${HOME}/iceberg-bench-data/staging-shred-v2/w5-clustered"
DEST="${1:-$DEFAULT_DEST}"
FORCE="${FORCE:-0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
LOADER="${SCRIPT_DIR}/gh_archive_load.sh"

if [[ ! -f "$LOADER" ]]; then
  echo "FAIL: gh_archive_load.sh not found at $LOADER" >&2
  exit 1
fi

echo "===== w5-clustered staging fetch ====="
echo " Destination: ${DEST}"
echo " Force re-download: ${FORCE}"
echo ""

if [[ "$FORCE" != "1" && -d "${DEST}/events" ]]; then
  count=$(find "${DEST}/events" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
  if [[ "$count" -gt 0 ]]; then
    bytes=$(find "${DEST}/events" -name '*.parquet' -exec stat -f%z {} \; 2>/dev/null | awk '{sum+=$1} END {print sum}')
    echo "OK: ${count} parquet file(s) already present, ${bytes} bytes total."
    echo "Skipping download. Set FORCE=1 to re-download."
    exit 0
  fi
  echo "Found empty events/ dir from a prior failed attempt; wiping."
  rm -rf "${DEST}/events"
fi

# Pre-flight: catch missing deps BEFORE starting a 5GB download
echo ">>> Pre-flight checks"
command -v python3 >/dev/null || { echo "FAIL: python3 not on PATH" >&2; exit 1; }
command -v curl >/dev/null || { echo "FAIL: curl not on PATH" >&2; exit 1; }

if [[ -f "${REPO_ROOT}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.venv/bin/activate"
  echo "  using venv at ${REPO_ROOT}/.venv"
fi

python3 -c "import pyarrow" 2>/dev/null \
  || { echo "FAIL: pyarrow not importable (run: uv pip install pyarrow)" >&2; exit 1; }
echo "  python3 + pyarrow + curl OK"
echo ""

mkdir -p "${DEST}"

echo ">>> Downloading 24 hourly files from data.gharchive.org (2024-01-01)"
echo "    ~5-6 GB total, expect 5-15 min depending on network"
echo ""
bash "$LOADER" 2024-01-01 "${DEST}/events"

count=$(find "${DEST}/events" -name '*.parquet' 2>/dev/null | wc -l | tr -d ' ')
if [[ "$count" -eq 0 ]]; then
  echo "FAIL: loader completed but no parquet files in ${DEST}/events" >&2
  exit 1
fi
bytes=$(find "${DEST}/events" -name '*.parquet' -exec stat -f%z {} \; 2>/dev/null | awk '{sum+=$1} END {print sum}')

echo ""
echo "===== Done ====="
echo " Files:    ${count} parquet"
echo " Size:     ${bytes} bytes"
echo " Location: ${DEST}/events"
echo ""
echo "Next: point your benchmark runner at ${DEST}/events (or symlink it into your staging tree)."
