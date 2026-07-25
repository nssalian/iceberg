#!/usr/bin/env bash
# Wraps a Python augmenter. Reads input parquet, writes output parquet.
# Each augmenter mutates the payload column per its failure-mode spec.
#
# Usage:
#   ./benchmark/scripts/augmenters/run.sh <augmenter> <input.parquet> <output.parquet>
#
# Augmenters: mixed_type, high_card, long_array, drift, wide_object, blob_payload,
#             polymorphic, deeply_nested
#
# Requires: python3, pyarrow on the host.

set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "Usage: $0 <augmenter> <input.parquet> <output.parquet>" >&2
  exit 1
fi

AUGMENTER="$1"
INPUT="$2"
OUTPUT="$3"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE="${SCRIPT_DIR}/${AUGMENTER}.py"

if [[ ! -f "$MODULE" ]]; then
  echo "Unknown augmenter: $AUGMENTER (expected ${MODULE})" >&2
  echo "Available: mixed_type, high_card, long_array, drift, wide_object, blob_payload, polymorphic, deeply_nested" >&2
  exit 1
fi

if [[ ! -f "$INPUT" ]]; then
  echo "Input not found: $INPUT" >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "FAIL: python3 not on PATH" >&2
  exit 1
fi

# If a repo-root .venv exists, activate it so matrix runs find pyarrow there
# regardless of caller's shell state. Falls through to system python if absent.
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
if [[ -f "${REPO_ROOT}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.venv/bin/activate"
fi

if ! python3 -c "import pyarrow" 2>/dev/null; then
  echo "FAIL: pyarrow not installed (uv pip install pyarrow, or pip install pyarrow)" >&2
  exit 1
fi

mkdir -p "$(dirname "$OUTPUT")"

echo "===== Augmenter: $AUGMENTER ====="
echo " Input:  $INPUT"
echo " Output: $OUTPUT"
echo ""

python3 "$MODULE" --input "$INPUT" --output "$OUTPUT"

if [[ ! -f "$OUTPUT" ]]; then
  echo "FAIL: augmenter ran but did not produce $OUTPUT" >&2
  exit 1
fi

rows=$(python3 -c "import pyarrow.parquet as p; print(p.read_metadata('$OUTPUT').num_rows)")
echo "OK: ${rows} rows written"
