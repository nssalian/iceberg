#!/usr/bin/env bash
# Downloads GitHub Archive hourly files for a pinned date and loads into parquet.
# Sorts events by `type` (clustering signal for W5).
#
# Usage:
#   ./benchmark/scripts/gh_archive_load.sh <YYYY-MM-DD> <output-dir>
#
# Notes:
#   ~5 GB compressed download for 24 hourly files; pinned to 2024-01-01 by default.

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <YYYY-MM-DD> <output-dir>" >&2
  exit 1
fi

DATE="$1"
OUT="$2"

if [[ ! "$DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "FAIL: date must be YYYY-MM-DD, got: $DATE" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOADER="${SCRIPT_DIR}/gh_archive_loader.py"

if [[ ! -f "$LOADER" ]]; then
  echo "FAIL: loader not found at $LOADER" >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "FAIL: python3 not on PATH" >&2
  exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "FAIL: curl required for download" >&2
  exit 1
fi

# If a repo-root .venv exists, activate it so matrix runs find pyarrow there
# regardless of caller's shell state.
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
if [[ -f "${REPO_ROOT}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.venv/bin/activate"
fi

if ! python3 -c "import pyarrow" 2>/dev/null; then
  echo "FAIL: pyarrow not installed (uv pip install pyarrow)" >&2
  exit 1
fi

mkdir -p "$OUT"

echo "===== GitHub Archive load: $DATE ====="
echo " Output: $OUT"
echo ""

python3 "$LOADER" --date "$DATE" --output "$OUT"

written=$(find "$OUT" -name '*.parquet' | wc -l | tr -d ' ')
if [[ "$written" -eq 0 ]]; then
  echo "FAIL: loader produced no parquet files in $OUT" >&2
  exit 1
fi

echo ""
echo "OK: wrote ${written} parquet files to $OUT"
