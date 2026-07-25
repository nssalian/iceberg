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
"""W2 high-cardinality augmenter.

Reads input parquet (event_id BIGINT, payload STRING) and produces output parquet
with the same schema. Replaces the payload with a single {request_id: <uuid>}
field per row. Every value is unique - this is the cardinality-gate trigger
workload for V2_CARDGATED to reject.

Deterministic: derives the uuid hex from event_id, not a random seed, so re-runs
produce identical files.
"""
import argparse
import json
import sys
import uuid

import pyarrow as pa
import pyarrow.parquet as pq


def make_unique_payload(event_id: int) -> str:
    """Build a {request_id: <hex>} where hex is derived from event_id. Unique per row."""
    # Use uuid5 with a fixed namespace so output is deterministic across runs.
    namespace = uuid.UUID("12345678-1234-5678-1234-567812345678")
    request_id = uuid.uuid5(namespace, str(event_id)).hex
    return json.dumps({"request_id": request_id}, separators=(",", ":"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Input parquet path")
    parser.add_argument("--output", required=True, help="Output parquet path")
    args = parser.parse_args()

    table = pq.read_table(args.input)
    event_ids = table.column("event_id").to_pylist()

    new_payloads = [make_unique_payload(eid) for eid in event_ids]

    new_table = pa.Table.from_arrays(
        [table.column("event_id"), pa.array(new_payloads, type=pa.string())],
        names=["event_id", "payload"],
    )
    pq.write_table(new_table, args.output, compression="zstd")
    print(f"OK: high_card wrote {new_table.num_rows} rows; every payload is a unique UUID",
          file=sys.stderr)


if __name__ == "__main__":
    main()
