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
"""W5 GitHub Archive loader.

Downloads 24 hourly files from https://data.gharchive.org/<DATE>-<HH>.json.gz,
parses each JSON event, SORTS BY `type` (so the output is clustered by event
type - the worst case for B5 first-20-uniform sampling), and writes parquet
files of shape (event_id BIGINT, payload STRING) at target file size.

Real-world data. Cluster-by-type makes B5 vulnerable: the first 20 rows of any
output file will all be the same type, so B5 will infer based on that schema -
then the rest of the file is mostly different events.

Output filename layout: <output-dir>/events/00000.parquet, 00001.parquet, ...

Env vars:
  TARGET_FILE_BYTES (default 50_000_000 = 50 MB): roughly the desired parquet file size.
  HOURS (default "all"): comma-separated list of hours (00-23) or "all" for 24 files.
"""
import argparse
import gzip
import io
import json
import os
import sys
import urllib.request
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

GH_ARCHIVE_URL = "https://data.gharchive.org/{date}-{hour}.json.gz"


def fetch_hour(date: str, hour: str) -> bytes:
    url = GH_ARCHIVE_URL.format(date=date, hour=hour)
    print(f"  downloading {url}", file=sys.stderr)
    req = urllib.request.Request(url, headers={"User-Agent": "iceberg-bench/1.0"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read()


def parse_events(gz_bytes: bytes):
    """Yield (event_type, payload_str) per event in the gz file."""
    with gzip.GzipFile(fileobj=io.BytesIO(gz_bytes)) as gz:
        for line in gz:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            event_type = event.get("type", "Unknown")
            yield event_type, line.decode("utf-8").rstrip("\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--output", required=True, help="Output directory")
    args = parser.parse_args()

    target_bytes = int(os.environ.get("TARGET_FILE_BYTES", "50000000"))
    hours_env = os.environ.get("HOURS", "all")
    if hours_env == "all":
        hours = [f"{h}" for h in range(24)]
    else:
        hours = [h.strip() for h in hours_env.split(",")]

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: download + parse all events; collect (event_type, payload) pairs.
    print(f"===== GH Archive load: date={args.date} hours={len(hours)} =====", file=sys.stderr)
    events = []  # list of (event_type, payload_str)
    for hour in hours:
        gz = fetch_hour(args.date, hour)
        for event_type, payload in parse_events(gz):
            events.append((event_type, payload))
        print(f"    cumulative events: {len(events)}", file=sys.stderr)

    # Step 2: sort by event type so output files cluster by type.
    print(f"  sorting {len(events)} events by event type for clustering...", file=sys.stderr)
    events.sort(key=lambda pair: pair[0])

    # Step 3: write files of ~target_bytes by accumulating payloads.
    print(f"  writing parquet files (target ~{target_bytes // (1024 * 1024)} MB each)...",
          file=sys.stderr)
    file_index = 0
    batch_event_ids = []
    batch_payloads = []
    batch_size = 0
    next_event_id = 0

    def flush():
        nonlocal file_index, batch_event_ids, batch_payloads, batch_size
        if not batch_payloads:
            return
        table = pa.Table.from_arrays(
            [pa.array(batch_event_ids, type=pa.int64()),
             pa.array(batch_payloads, type=pa.string())],
            names=["event_id", "payload"],
        )
        out_path = out_dir / f"{file_index:05d}.parquet"
        pq.write_table(table, out_path, compression="zstd")
        print(f"    wrote {out_path.name}: {table.num_rows} rows, {batch_size} payload bytes",
              file=sys.stderr)
        file_index += 1
        batch_event_ids = []
        batch_payloads = []
        batch_size = 0

    for _event_type, payload in events:
        batch_event_ids.append(next_event_id)
        batch_payloads.append(payload)
        batch_size += len(payload)
        next_event_id += 1
        if batch_size >= target_bytes:
            flush()
    flush()

    print(f"===== Done: {file_index} files in {out_dir} =====", file=sys.stderr)


if __name__ == "__main__":
    main()
