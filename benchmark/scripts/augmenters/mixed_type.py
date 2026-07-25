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
"""W3 mixed-type augmenter.

Reads input parquet (event_id BIGINT, payload STRING) and produces output parquet
with the same schema, but a chosen JSON field flipped to STRING with probability
1 - INT_RATIO. The default field is `duration` (an INT scalar in the staged JSON).

Env vars:
  INT_RATIO (default 0.60): fraction of rows that keep the field as INT.
  TARGET_FIELD (default "duration"): which top-level field to mutate.

This produces the W3-mixed-* workload corpus. Determinism: fixed seed for the
INT/STRING choice so re-runs produce identical files.
"""
import argparse
import json
import os
import random
import sys

import pyarrow as pa
import pyarrow.parquet as pq

SEED = 0x1ceb1ceb


def mutate(payload_str: str, flip_to_string: bool, target_field: str, row_index: int) -> str:
    """Parse the JSON, replace target_field with a STRING if flip_to_string is True."""
    try:
        payload = json.loads(payload_str)
    except json.JSONDecodeError:
        return payload_str
    if target_field not in payload:
        return payload_str
    if flip_to_string:
        payload[target_field] = f"str_{row_index}"
    return json.dumps(payload, separators=(",", ":"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Input parquet path")
    parser.add_argument("--output", required=True, help="Output parquet path")
    args = parser.parse_args()

    int_ratio = float(os.environ.get("INT_RATIO", "0.60"))
    target_field = os.environ.get("TARGET_FIELD", "duration")
    if not (0.0 <= int_ratio <= 1.0):
        print(f"FAIL: INT_RATIO must be in [0, 1], got {int_ratio}", file=sys.stderr)
        sys.exit(1)

    rng = random.Random(SEED)
    table = pq.read_table(args.input)
    payloads = table.column("payload").to_pylist()

    new_payloads = []
    for i, payload_str in enumerate(payloads):
        keep_int = rng.random() < int_ratio
        new_payloads.append(mutate(payload_str, not keep_int, target_field, i))

    new_table = pa.Table.from_arrays(
        [table.column("event_id"), pa.array(new_payloads, type=pa.string())],
        names=["event_id", "payload"],
    )
    pq.write_table(new_table, args.output, compression="zstd")
    print(
        f"OK: mixed_type wrote {new_table.num_rows} rows; "
        f"INT_RATIO={int_ratio}, TARGET_FIELD={target_field}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
