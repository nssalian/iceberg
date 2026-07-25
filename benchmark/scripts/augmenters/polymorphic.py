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
"""Polymorphic augmenter (Phase 3.5).

Adds a `data` key whose value type varies row-by-row across four shapes with
equal probability: object, array, int scalar, string scalar. Stresses the
type-uniformity gate (V2_UNIFORM rejects, B1_MAJORITY picks the most common).
Existing top-level fields are preserved.
"""
import argparse
import json
import random
import sys

import pyarrow as pa
import pyarrow.parquet as pq

SEED = 42
SHAPES = ("object", "array", "int", "string")


def build_data(rng: random.Random) -> object:
    shape = rng.choice(SHAPES)
    if shape == "object":
        return {"a": 1, "b": 2}
    elif shape == "array":
        return [1, 2, 3]
    elif shape == "int":
        return 42
    else:
        return "hello"


def mutate(payload_str: str, row_index: int) -> str:
    try:
        payload = json.loads(payload_str)
    except json.JSONDecodeError:
        return payload_str
    rng = random.Random(SEED ^ row_index)
    payload["data"] = build_data(rng)
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
        f"OK: polymorphic wrote {new_table.num_rows} rows; "
        f"data shape uniform over {SHAPES}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
