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
"""Deeply nested augmenter (Phase 3.5).

Adds a `config` key with 8 levels of nesting (l0..l7), one child per level,
to each row's payload. Tests MAX_SHREDDING_DEPTH and per-row traversal cost.
Existing top-level fields are preserved.
"""
import argparse
import json
import random
import sys

import pyarrow as pa
import pyarrow.parquet as pq

SEED = 42
DEPTH = 8


def build_config(rng: random.Random) -> dict:
    """Build {"l0":{"l1":{...{"l7":{"v": <int>}}}}} with the leaf int seeded."""
    leaf = {"v": rng.randint(0, 999999)}
    node = leaf
    for level in range(DEPTH - 1, -1, -1):
        node = {f"l{level}": node}
    return node


def mutate(payload_str: str, row_index: int) -> str:
    try:
        payload = json.loads(payload_str)
    except json.JSONDecodeError:
        return payload_str
    rng = random.Random(SEED ^ row_index)
    payload["config"] = build_config(rng)
    return json.dumps(payload, separators=(",", ":"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Input parquet path")
    parser.add_argument("--output", required=True, help="Output parquet path")
    args = parser.parse_args()

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
        f"OK: deeply_nested wrote {new_table.num_rows} rows; "
        f"config nested {DEPTH} levels deep with seeded leaf integer",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
