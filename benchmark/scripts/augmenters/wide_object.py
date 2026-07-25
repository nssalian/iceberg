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
"""Wide-object augmenter (Phase 3.5).

Adds 200 top-level keys (k001..k200) to each row's payload. Half are present per
row (seeded RNG decides which) so the analyzer must handle a wide, sparse object
schema. Existing top-level fields are preserved.
"""
import argparse
import json
import os
import random
import sys

import pyarrow as pa
import pyarrow.parquet as pq

SEED = 42
KEY_COUNT = 200
KEY_NAMES = [f"k{i:03d}" for i in range(1, KEY_COUNT + 1)]


def mutate(payload_str: str, row_index: int) -> str:
    """Merge KEY_COUNT/2 randomly-chosen sparse keys into the payload."""
    try:
        payload = json.loads(payload_str)
    except json.JSONDecodeError:
        return payload_str
    rng = random.Random(SEED ^ row_index)
    selected = rng.sample(KEY_NAMES, KEY_COUNT // 2)
    for key in selected:
        payload[key] = rng.randint(0, 999999)
    return json.dumps(payload, separators=(",", ":"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Input parquet path")
    parser.add_argument("--output", required=True, help="Output parquet path")
    args = parser.parse_args()

    key_count = int(os.environ.get("KEY_COUNT", str(KEY_COUNT)))
    if key_count <= 0:
        print(f"FAIL: KEY_COUNT must be positive, got {key_count}", file=sys.stderr)
        sys.exit(1)

    table = pq.read_table(args.input)
    event_ids = table.column("event_id").to_pylist()
    payloads = table.column("payload").to_pylist()

    new_payloads = [mutate(payload_str, eid) for eid, payload_str in zip(event_ids, payloads)]

    new_table = pa.Table.from_arrays(
        [table.column("event_id"), pa.array(new_payloads, type=pa.string())],
        names=["event_id", "payload"],
    )
    pq.write_table(new_table, args.output, compression="zstd")
    print(
        f"OK: wide_object wrote {new_table.num_rows} rows; "
        f"each row gets {KEY_COUNT // 2} of {KEY_COUNT} top-level keys",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
