#!/usr/bin/env bash
# Collect metrics from benchmark result directories into a single CSV summary.
# Usage: collect-metrics.sh <results_dir>

set -euo pipefail

RESULTS_DIR="${1:?Usage: collect-metrics.sh <results_dir>}"
OUTPUT="${RESULTS_DIR}/summary.csv"

# Read engine from run metadata if available
ENGINE="unknown"
if [[ -f "${RESULTS_DIR}/run-metadata.json" ]] && command -v python3 &>/dev/null; then
  ENGINE=$(python3 -c "import json; print(json.load(open('${RESULTS_DIR}/run-metadata.json'))['engine'])" 2>/dev/null || echo "unknown")
fi

echo "engine,operation,format,compression,iteration,wall_clock_ms" > "$OUTPUT"

for timing_file in $(find "$RESULTS_DIR" -name "timing.json" -type f | sort); do
  # Path layout: <results_dir>/<operation>/<format-compression>/timing.json
  rel_path="${timing_file#${RESULTS_DIR}/}"
  operation=$(echo "$rel_path" | cut -d'/' -f1)
  format_comp=$(echo "$rel_path" | cut -d'/' -f2)
  format=$(echo "$format_comp" | cut -d'-' -f1)
  compression=$(echo "$format_comp" | cut -d'-' -f2-)

  if command -v python3 &>/dev/null; then
    python3 -c "
import json
with open('$timing_file') as f:
    data = json.load(f)
for entry in data:
    print(f'$ENGINE,$operation,$format,$compression,{entry[\"iteration\"]},{entry[\"wall_clock_ms\"]}')
" >> "$OUTPUT"
  elif command -v jq &>/dev/null; then
    jq -r ".[] | \"$ENGINE,$operation,$format,$compression,\" + (.iteration|tostring) + \",\" + (.wall_clock_ms|tostring)" "$timing_file" >> "$OUTPUT"
  else
    echo "Warning: neither python3 nor jq found, skipping $timing_file" >&2
  fi
done

echo "Summary written to: $OUTPUT"
wc -l "$OUTPUT"
