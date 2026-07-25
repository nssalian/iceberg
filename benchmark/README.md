# Iceberg Variant Baseline Benchmark


Measures the cost of reading and writing Iceberg Variant columns compared to JSON-as-string columns on identical data. Includes a shredded variant table using `write.parquet.shred-variants=true` (PR [#14297](https://github.com/apache/iceberg/pull/14297), now in apache/main).

## Quick Start

```bash
# Build
./gradlew :iceberg-benchmark:compileJava -DsparkVersions=4.1 -DflinkVersions= -DscalaVersion=2.13

# Generate 10M rows of staged data
./gradlew :iceberg-benchmark:generateData -DsparkVersions=4.1 -DflinkVersions= -DscalaVersion=2.13 -Dbench.scale=small -Dbench.staging=/tmp/iceberg-bench/staging-v2-small

# Run writes + reads (small)
./benchmark/scripts/run_spark.sh small
```

### Data Generation (all scales)

```bash
# Small (10M rows, ~700 MB)
./gradlew :iceberg-benchmark:generateData -DsparkVersions=4.1 -DflinkVersions= -DscalaVersion=2.13 -Dbench.scale=small -Dbench.staging=/tmp/iceberg-bench/staging-v2-small

# Medium (100M rows, ~7 GB)
./gradlew :iceberg-benchmark:generateData -DsparkVersions=4.1 -DflinkVersions= -DscalaVersion=2.13 -Dbench.scale=medium -Dbench.staging=/tmp/iceberg-bench/staging-v2-medium

# Large (500M rows, ~35 GB)
./gradlew :iceberg-benchmark:generateData -DsparkVersions=4.1 -DflinkVersions= -DscalaVersion=2.13 -Dbench.scale=large -Dbench.staging=/tmp/iceberg-bench/staging-v2-large
```

### Running Benchmarks

```bash
# Run writes + reads at a given scale (generates data if missing)
./benchmark/scripts/run_spark.sh small
./benchmark/scripts/run_spark.sh medium
./benchmark/scripts/run_spark.sh large

# Run specific operations only via bench.sh
./benchmark/scripts/bench.sh --engine spark --scale small --staging /tmp/iceberg-bench/staging-v2-small --skip-datagen --ops "write-json,write-variant,write-shredded" --warmup 1 --iterations 3 --tag "writes-only"

./benchmark/scripts/bench.sh --engine spark --scale small --staging /tmp/iceberg-bench/staging-v2-small --skip-datagen --ops "read-baseline,read-project-json,read-project-variant,read-project-shredded" --warmup 2 --iterations 5 --tag "reads-only"

# Quick smoke test (rebuilds warehouse, runs write-shredded once)
./benchmark/scripts/verify.sh
```

### Available Operations

**Writes:** `write-json`, `write-variant`, `write-shredded`

**Reads:** `read-baseline`, `read-project-json`, `read-project-variant`, `read-project-shredded`, `read-nested-json`, `read-nested-variant`, `read-nested-shredded`, `read-filter-json`, `read-filter-variant`, `read-filter-shredded`, `read-agg-json`, `read-agg-variant`, `read-agg-shredded`

## Design

**Schema:** 2-column Iceberg tables - `event_id BIGINT, payload STRING/VARIANT`. Payload is nested JSON with flat scalars, 2-level objects, arrays, booleans, mixed types.

**Tables:**
- `events_json` - payload as STRING
- `events_variant` - payload as VARIANT (unshredded)
- `events_shredded` - payload as VARIANT with `write.parquet.shred-variants=true` (auto-inferred fields, default 100-row buffer)

All tables use format-version 3, Parquet/zstd, vectorization disabled.

**Writes ARE the data loading.** Zero untimed INSERTs. Write benchmarks populate the tables, read benchmarks query them.

**Reads** compare `get_json_object()` on STRING vs `try_variant_get()` on VARIANT (unshredded and shredded) across four query patterns: flat projection, nested projection, filter, and aggregation.

## Results (Spark 4.1)

**Environment:** macOS, 36 GB RAM, Java 17, local filesystem.

Latest run: 2026-06-02 (small + medium). See [REPORT.md](REPORT.md) for full history (Phase 0 baseline + Phase 1 with shredding) at both scales.

### Writes (10M rows, median ms)

| Operation | ms | vs JSON |
|-----------|---:|--------:|
| write-json | 2,476 | 1.0x |
| write-variant | 8,280 | 3.3x |
| write-shredded | 19,648 | 7.9x |

### Reads (10M rows, median ms)

| Query Pattern | JSON | Variant | Shredded |
|---|---:|---:|---:|
| read-baseline (typed BIGINT) | 160 | - | - |
| Projection (flat) | 2,964 | 2,581 | 12,985 |
| Projection (nested) | 3,036 | 2,819 | 13,677 |
| Filter | 3,446 | 2,969 | 13,469 |
| Aggregation | 5,409 | 3,592 | 14,517 |

Shredded reads on stock apache/main are 4-5x slower than unshredded variant on this workload (default auto-inference, 100-row buffer).

## Files

```
benchmark/
  README.md                    # This file
  REPORT.md                    # Run history (Phase 0 baseline, Phase 1 + shredding)
  build.gradle                 # Gradle module with JavaExec tasks
  scripts/
    run_spark.sh               # Run writes + reads at a given scale
    bench.sh                   # Single benchmark run wrapper (timestamped)
    env.sh                     # Environment config
    verify.sh                  # Quick smoke test (write-shredded only)
  src/main/java/.../
    DataGenerator.java         # Generates 2-column staged Parquet
    SparkBenchDriver.java      # Write + read benchmarks
    BenchmarkMetricsReporter.java
    BenchmarkResultWriter.java
  results/
    collect-metrics.sh         # Aggregate timing.json -> summary.csv
    report.py                  # Summary stats + comparison
  runs/                        # Timestamped result directories
```

## Configuration

All `bench.*` system properties are forwarded to the driver JVM:

| Property | Default | Description |
|----------|---------|-------------|
| bench.scale | small | small (10M), medium (100M), large (500M) |
| bench.staging | /tmp/iceberg-bench/staging-v2-small | Staged Parquet directory |
| bench.warehouse | /tmp/iceberg-bench/warehouse/spark | Iceberg warehouse |
| bench.threads | 4 | Spark local thread count |
| bench.warmup | 2 | Warmup iterations (discarded) |
| bench.iterations | 5 | Measured iterations |
| bench.compression | zstd | Parquet compression codec |
| bench.operations | all | Comma-separated ops, or "all" |

## Related Work

- [Qiegang Long - Variant Conformance Benchmark](https://qlong.github.io/posts/2026-03-30-variant-early-results/)
- [Steve Loughran - Benchmarking Parquet Variants](https://steveloughran.github.io/benchmarking-variants/benchmarking-variants.html)
- [Iceberg Variant umbrella issue #10392](https://github.com/apache/iceberg/issues/10392)

---

## Methodology

### Tables

Two Iceberg tables with identical 2-column schemas. Only difference: the type of `payload`.

**events_json:**
```sql
CREATE TABLE events_json (
  event_id BIGINT,
  payload  STRING
) USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'zstd',
  'write.metadata.compression-codec' = 'gzip',
  'read.parquet.vectorization.enabled' = 'false'
)
```

**events_variant:**
```sql
CREATE TABLE events_variant (
  event_id BIGINT,
  payload  VARIANT
) USING iceberg
TBLPROPERTIES (
  -- identical to events_json
)
```

### Fairness Controls

| Property | Value | Rationale |
|----------|-------|-----------|
| format-version | 3 on both | Variant requires v3. JSON table uses v3 too to avoid format-version confound. |
| vectorization | disabled on both | Variant disables vectorization automatically. JSON table must match to isolate payload format cost, not vectorization benefit. |
| compression | zstd on both | Same codec, same overhead. |
| schema | 2 columns on both | No extra columns to dilute the comparison. |

### Payload Content

Every row's `payload` is a nested JSON string:

```json
{
  "event_type": "purchase",
  "country": "US",
  "duration": 4821,
  "user": {
    "id": 12345,
    "is_premium": true,
    "session": "a1b2c3d4"
  },
  "metrics": {
    "timing": {
      "load_ms": 1234,
      "render_ms": 5678
    },
    "flags": {
      "is_bot": false
    }
  },
  "tags": ["tag_12", "tag_37"],
  "label": "item_2345"
}
```

Flat scalars (string, int), 2-level nested objects, arrays, booleans, mixed types.

**Data parity:** `events_json` stores this as the literal string. `events_variant` stores it as `parse_json(payload)` - same JSON, different encoding.

### Distributions

| Field | Distribution |
|-------|-------------|
| event_id | Sequential 0 to N |
| event_type | 20 values, 80/20 skew (top 4 get 80% of rows) |
| country | 50 country codes, uniform |
| duration | Uniform 0-299,999 |
| user.id | Uniform mod 10M |
| user.is_premium | 15% true |
| metrics.timing.load_ms | Uniform 0-4,999 |
| metrics.flags.is_bot | 5% true |

### Benchmark Flow

```
Step 1: DataGenerator (separate Gradle task)
   -> staged Parquet files: event_id + payload STRING

Step 2: SparkBenchDriver
   createTables()        // CREATE TABLE IF NOT EXISTS (empty)
   runWriteBenchmarks()  // TIMED: populates tables
   runReadBenchmarks()   // TIMED: queries populated tables
```

**Writes are the data loading.** Zero untimed INSERTs. Writes and reads run as separate JVMs. OS page cache cleared between runs.

### Write Queries

1 warmup + 3 measured iterations. Table dropped and recreated between iterations.

```sql
-- write-json
INSERT INTO events_json SELECT event_id, payload FROM staged_events

-- write-variant
INSERT INTO events_variant SELECT event_id, parse_json(payload) FROM staged_events
```

### Read Queries

2 warmup + 5 measured iterations, median reported.

**Baseline:**
```sql
SELECT SUM(event_id) FROM events_json
```

**Projection - flat field:**
```sql
-- json
SELECT SUM(CAST(get_json_object(payload, '$.duration') AS INT)) FROM events_json

-- variant
SELECT SUM(try_variant_get(payload, '$.duration', 'int')) FROM events_variant
```

**Projection - nested field (2 levels):**
```sql
-- json
SELECT SUM(CAST(get_json_object(payload, '$.metrics.timing.load_ms') AS INT)) FROM events_json

-- variant
SELECT SUM(try_variant_get(payload, '$.metrics.timing.load_ms', 'int')) FROM events_variant
```

**Filter:**
```sql
-- json
SELECT COUNT(event_id),
       SUM(CAST(get_json_object(payload, '$.duration') AS INT))
FROM events_json
WHERE get_json_object(payload, '$.event_type') = 'purchase'
  AND get_json_object(payload, '$.country') = 'US'

-- variant
SELECT COUNT(event_id),
       SUM(try_variant_get(payload, '$.duration', 'int'))
FROM events_variant
WHERE try_variant_get(payload, '$.event_type', 'string') = 'purchase'
  AND try_variant_get(payload, '$.country', 'string') = 'US'
```

**Aggregation:**
```sql
-- json
SELECT get_json_object(payload, '$.event_type'),
       COUNT(event_id),
       SUM(CAST(get_json_object(payload, '$.duration') AS INT))
FROM events_json
GROUP BY get_json_object(payload, '$.event_type')

-- variant
SELECT try_variant_get(payload, '$.event_type', 'string'),
       COUNT(event_id),
       SUM(try_variant_get(payload, '$.duration', 'int'))
FROM events_variant
GROUP BY try_variant_get(payload, '$.event_type', 'string')
```

### Known Limitations

1. **No predicate pushdown.** Neither JSON nor variant can skip row groups for payload field predicates.
2. **`get_json_object` requires CAST.** JSON extraction returns STRING; this is real-world behavior, not a benchmark artifact.
3. **Single-node local filesystem.** No HDFS, no S3.
4. **Synthetic dataset.** Controlled distributions, not real-world data.
5. **Warm JVM within a run.** All reads share a JVM. Writes and reads use separate JVMs.
