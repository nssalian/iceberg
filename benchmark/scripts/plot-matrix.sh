#!/usr/bin/env bash
# Wrapper for plot-matrix.py. Sources the repo .venv and runs the plotter.
#
# Usage: ./benchmark/scripts/plot-matrix.sh <results-dir>

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <results-dir>" >&2
  exit 1
fi

RESULTS="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ ! -f "${RESULTS}/scoreboard.csv" ]]; then
  echo "FAIL: ${RESULTS}/scoreboard.csv not found. Run score-matrix.sh first." >&2
  exit 1
fi

if [[ -f "${REPO_ROOT}/.venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "${REPO_ROOT}/.venv/bin/activate"
fi

if ! python3 -c "import pandas, matplotlib" 2>/dev/null; then
  echo "FAIL: pandas + matplotlib required. Run: uv pip install pandas matplotlib" >&2
  exit 1
fi

python3 "${SCRIPT_DIR}/plot-matrix.py" "$RESULTS"
