#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

usage() {
  cat >&2 <<EOF
Usage: $0 <parquet-file> <variant-column> <shredded-field> [outputDir] [workloadLabel] [strategyLabel] [projectFields] [filterField] [aggField]

  <parquet-file>    absolute path to a shredded parquet data file
  <variant-column>  top-level variant column name (e.g. payload)
  <shredded-field>  sub-field to probe (e.g. duration, event_type)
  [outputDir]       CSV output directory, defaults to benchmark/results
  [workloadLabel]   label recorded in CSV, defaults to unknown
  [strategyLabel]   label recorded in CSV, defaults to unknown
  [projectFields]   comma-separated fields to project (defaults to <shredded-field>)
  [filterField]     top-level primitive field for filter+project path (defaults to first projectField)
  [aggField]        top-level primitive numeric field for SUM aggregate path (defaults to first projectField)
  [arraysFixture]   path to arrays_shredded parquet for Snowflake Q6-Q11 reader-only paths (optional)
EOF
  exit 1
}

[ $# -ge 3 ] || usage

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

FIXTURE="$1"
COLUMN="$2"
FIELD="$3"
OUT_DIR="${4:-benchmark/results}"
# resolve OUT_DIR relative to REPO_ROOT so the gradle task (which runs with cwd=benchmark/) writes to the right place
if [[ "$OUT_DIR" != /* ]]; then
  OUT_DIR="$REPO_ROOT/$OUT_DIR"
fi
WORKLOAD="${5:-unknown}"
STRATEGY="${6:-unknown}"
PROJECT_FIELDS="${7:-$FIELD}"
FILTER_FIELD="${8:-}"
AGG_FIELD="${9:-}"
ARRAYS_FIXTURE="${10:-}"

if [ ! -f "$FIXTURE" ]; then
  echo "error: fixture parquet file not found: $FIXTURE" >&2
  exit 2
fi

cd "$REPO_ROOT"

# Build gradle args conditionally; when trailing args are empty the Java main uses defaults.
QC_ARGS="$FIXTURE $COLUMN $FIELD $OUT_DIR $WORKLOAD $STRATEGY $PROJECT_FIELDS"
if [ -n "$FILTER_FIELD" ]; then
  QC_ARGS="$QC_ARGS $FILTER_FIELD"
  if [ -n "$AGG_FIELD" ]; then
    QC_ARGS="$QC_ARGS $AGG_FIELD"
    if [ -n "$ARRAYS_FIXTURE" ]; then
      QC_ARGS="$QC_ARGS $ARRAYS_FIXTURE"
    fi
  fi
fi

./gradlew :iceberg-benchmark:variantQuickCheck \
  -DsparkVersions= -DflinkVersions= -DscalaVersion=2.13 \
  -PquickCheckArgs="$QC_ARGS" \
  --parallel --max-workers=4
