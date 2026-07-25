#!/usr/bin/env python3
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
"""W6 drift augmenter (CONDITIONAL).

Reads input parquet (event_id BIGINT, payload STRING) and produces output parquet
with the same schema. The first DRIFT_AT rows are passed through unchanged. After
row DRIFT_AT, a new `trace_id` field appears in every payload.

Strategies that lock the schema from an early sample (B4 row 0, B5 first 20) will
NEVER see trace_id and won't shred it. V2-uniform and V2-cardgated walk the whole
file and DO see it. This is the workload that would distinguish adaptive sampling
(V2-full) from non-adaptive, IF V2-full ever ships.

Only runs when ENABLE_W6=1; matches the `ensure-workload.sh` gating.

Env vars:
  DRIFT_AT (default 50000): row index at which trace_id starts appearing.
"""
import argparse
import json
import os
import sys
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

NAMESPACE = uuid.UUID("87654321-4321-8765-4321-876543218765")


def mutate(payload_str: str, event_id: int) -> str:
    try:
        payload = json.loads(payload_str)
    except json.JSONDecodeError:
        return payload_str
    payload["trace_id"] = uuid.uuid5(NAMESPACE, str(event_id)).hex
    return json.dumps(payload, separators=(",", ":"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Input parquet path")
    parser.add_argument("--output", required=True, help="Output parquet path")
    args = parser.parse_args()

    drift_at = int(os.environ.get("DRIFT_AT", "50000"))
    if drift_at < 0:
        print(f"FAIL: DRIFT_AT must be non-negative, got {drift_at}", file=sys.stderr)
        sys.exit(1)

    table = pq.read_table(args.input)
    event_ids = table.column("event_id").to_pylist()
    payloads = table.column("payload").to_pylist()

    new_payloads = []
    drifted = 0
    for i, (eid, payload_str) in enumerate(zip(event_ids, payloads)):
        if i >= drift_at:
            new_payloads.append(mutate(payload_str, eid))
            drifted += 1
        else:
            new_payloads.append(payload_str)

    new_table = pa.Table.from_arrays(
        [table.column("event_id"), pa.array(new_payloads, type=pa.string())],
        names=["event_id", "payload"],
    )
    pq.write_table(new_table, args.output, compression="zstd")
    print(
        f"OK: drift wrote {new_table.num_rows} rows; "
        f"trace_id appears from row {drift_at} onwards ({drifted} rows)",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
