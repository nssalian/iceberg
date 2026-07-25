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
"""Blob-payload augmenter (Phase 3.5).

Adds a single `notes` field containing 10-50KB of text per row. Tests the
variant writer's behavior with one dominant large blob field competing with
normal-sized scalars. Existing top-level fields are preserved.
"""
import argparse
import json
import os
import random
import sys

import pyarrow as pa
import pyarrow.parquet as pq

SEED = 42
MIN_BYTES = 10_000
MAX_BYTES = 50_000
CORPUS = (
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod "
    "tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim "
    "veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea "
    "commodo consequat. Duis aute irure dolor in reprehenderit in voluptate "
    "velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint "
    "occaecat cupidatat non proident, sunt in culpa qui officia deserunt "
    "mollit anim id est laborum. "
)


def mutate(payload_str: str, row_index: int) -> str:
    """Append a notes blob of deterministic size derived from row_index."""
    try:
        payload = json.loads(payload_str)
    except json.JSONDecodeError:
        return payload_str
    rng = random.Random(SEED ^ row_index)
    target_bytes = rng.randint(MIN_BYTES, MAX_BYTES)
    repeats = (target_bytes // len(CORPUS)) + 1
    blob = (CORPUS * repeats)[:target_bytes]
    payload["notes"] = blob
    return json.dumps(payload, separators=(",", ":"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Input parquet path")
    parser.add_argument("--output", required=True, help="Output parquet path")
    args = parser.parse_args()

    min_bytes = int(os.environ.get("MIN_BYTES", str(MIN_BYTES)))
    max_bytes = int(os.environ.get("MAX_BYTES", str(MAX_BYTES)))
    if min_bytes <= 0 or max_bytes < min_bytes:
        print(
            f"FAIL: MIN_BYTES must be positive and <= MAX_BYTES; got {min_bytes}, {max_bytes}",
            file=sys.stderr,
        )
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
        f"OK: blob_payload wrote {new_table.num_rows} rows; "
        f"notes blob size in [{MIN_BYTES}, {MAX_BYTES}] bytes",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
