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
"""W4 long-array augmenter.

Reads input parquet (event_id BIGINT, payload STRING) and produces output parquet
with the same schema. For 5% of rows (deterministic by event_id % 20 == 0), adds
an `events` array with 1000 elements like {id, qty, price} to the existing payload.
Other rows pass through unchanged.

This is the workload that distinguishes per-element vs per-row accounting in B1
(qlong bug, PR #14297 line 166). Fields inside the array appear ~5000x in the
denominator under B1's per-element counting but only 5% under per-row.

Env vars:
  ARRAY_LENGTH (default 1000): elements per long array.
  TRIGGER_MOD (default 20): one in this many rows gets the long array.
"""
import argparse
import json
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq


def build_events(row_index: int, length: int) -> list:
    """Generate the long array payload deterministically from row_index."""
    return [
        {"id": row_index * 1000 + j, "qty": (row_index + j) % 50, "price": float((j % 100) / 10.0)}
        for j in range(length)
    ]


def mutate(payload_str: str, row_index: int, array_length: int) -> str:
    try:
        payload = json.loads(payload_str)
    except json.JSONDecodeError:
        return payload_str
    payload["events"] = build_events(row_index, array_length)
    return json.dumps(payload, separators=(",", ":"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Input parquet path")
    parser.add_argument("--output", required=True, help="Output parquet path")
    args = parser.parse_args()

    array_length = int(os.environ.get("ARRAY_LENGTH", "1000"))
    trigger_mod = int(os.environ.get("TRIGGER_MOD", "20"))
    if array_length <= 0 or trigger_mod <= 0:
        print(f"FAIL: ARRAY_LENGTH and TRIGGER_MOD must be positive", file=sys.stderr)
        sys.exit(1)

    table = pq.read_table(args.input)
    event_ids = table.column("event_id").to_pylist()
    payloads = table.column("payload").to_pylist()

    new_payloads = []
    augmented = 0
    for eid, payload_str in zip(event_ids, payloads):
        if eid % trigger_mod == 0:
            new_payloads.append(mutate(payload_str, eid, array_length))
            augmented += 1
        else:
            new_payloads.append(payload_str)

    new_table = pa.Table.from_arrays(
        [table.column("event_id"), pa.array(new_payloads, type=pa.string())],
        names=["event_id", "payload"],
    )
    pq.write_table(new_table, args.output, compression="zstd")
    print(
        f"OK: long_array wrote {new_table.num_rows} rows; "
        f"{augmented} ({100.0 * augmented / new_table.num_rows:.2f}%) have a {array_length}-element events array",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
