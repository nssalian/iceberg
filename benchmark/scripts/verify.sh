#!/usr/bin/env bash
# Quick smoke test - runs write-shredded once at small scale.
# Use this after rebuild to confirm the shredded path works end-to-end.
set -euo pipefail
rm -rf /tmp/iceberg-bench/warehouse-v2-small
./gradlew :iceberg-benchmark:sparkBench -DsparkVersions=4.1 -DflinkVersions= -DscalaVersion=2.13 \
  -Dbench.warehouse=/tmp/iceberg-bench/warehouse-v2-small \
  -Dbench.staging=/tmp/iceberg-bench/staging-v2-small \
  -Dbench.results=/tmp/iceberg-bench/write-profile \
  -Dbench.operations=write-shredded \
  -Dbench.warmup=0 \
  -Dbench.iterations=1 2>&1 | grep -E "Median|--- Running"
