#!/usr/bin/env bash
# Wrapper for aggregate-footer-scores.py that uses the repo's .venv Python (with pyarrow installed).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

VENV_PY="$REPO_ROOT/.venv/bin/python"
if [ ! -x "$VENV_PY" ]; then
  echo "error: $VENV_PY not found. Run 'uv sync' or install pyarrow into $REPO_ROOT/.venv/" >&2
  exit 1
fi

exec "$VENV_PY" "$SCRIPT_DIR/aggregate-footer-scores.py" "$@"
