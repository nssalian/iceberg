# Variant Benchmark - Run History

Apache Iceberg V3 Variant - read/write performance vs JSON-as-string baseline.

**Environment:** macOS, 36 GB RAM, Java 17, Spark 4.1, local filesystem, format-version 3, Parquet/zstd, vectorization disabled.

**Schema:** 2-column tables - `event_id BIGINT`, `payload STRING|VARIANT`. Payload is nested JSON (flat scalars, 2-level nested objects, arrays, mixed types).

**Methodology:** writes 1 warmup + 3 measured iterations; reads 2 warmup + 5 measured iterations; median reported. Each run is its own JVM. Tables: `events_json`, `events_variant`, and (Phase 1) `events_shredded`.

---

## Phase 1 - apache/main + shredding writer (2026-06-02) — current

**Code:** `apache/main` HEAD `26a57711d9` + benchmark Shape A commit. Adds `events_shredded` table with `write.parquet.shred-variants=true` (PR [#14297](https://github.com/apache/iceberg/pull/14297)). Auto-inference, default 100-row buffer.

**New operations:** `write-shredded`, `read-{project,nested,filter,agg}-shredded` (5 added; 16 total).

### Small (10M rows)

**Writes (median ms):**

| Operation | ms | vs JSON | Phase 0 → Phase 1 |
|-----------|---:|--------:|------------------:|
| write-json | 2,476 | 1.0x | 2,389 → 2,476 (+3.6%) |
| write-variant | 8,280 | 3.3x | 7,971 → 8,280 (+3.9%) |
| write-shredded | 19,648 | 7.9x | NEW |

**Reads (median ms):**

| Query Pattern | JSON | Variant | Shredded | Shredded vs Variant |
|---|---:|---:|---:|---:|
| read-baseline (typed BIGINT) | 160 | - | - | - |
| Projection (flat) | 2,964 | 2,581 | 12,985 | 5.0x slower |
| Projection (nested) | 3,036 | 2,819 | 13,677 | 4.9x slower |
| Filter | 3,446 | 2,969 | 13,469 | 4.5x slower |
| Aggregation | 5,409 | 3,592 | 14,517 | 4.0x slower |

JSON and unshredded variant numbers are within ~5% of Phase 0 - methodology is stable across the two runs. Shredded is the new variable: writes ~2.4x slower than unshredded variant; reads 4-5x slower than unshredded variant on stock apache/main.

### Medium (100M rows)

**Writes (median ms):**

| Operation | ms | vs JSON | Phase 0 → Phase 1 |
|-----------|---:|--------:|------------------:|
| write-json | 17,996 | 1.0x | 17,345 → 17,996 (+3.8%) |
| write-variant | 69,677 | 3.9x | 72,501 → 69,677 (-3.9%) |
| write-shredded | 169,759 | 9.4x | NEW |

**Reads (median ms):**

| Query Pattern | JSON | Variant | Shredded | Shredded vs Variant |
|---|---:|---:|---:|---:|
| read-baseline (typed BIGINT) | 1,091 | - | - | - |
| Projection (flat) | 26,676 | 20,974 | 112,593 | 5.4x slower |
| Projection (nested) | 27,040 | 24,073 | 113,556 | 4.7x slower |
| Filter | 32,270 | 24,611 | 115,833 | 4.7x slower |
| Aggregation | 50,571 | 29,743 | 122,496 | 4.1x slower |

Shredded regression at 100M: 4.1-5.4x slower than unshredded variant across all four query patterns - same shape as 10M (4.0-5.0x). Write overhead grows from 7.9x (10M) to 9.4x (100M) vs JSON; per-row shredded write cost is roughly stable across scales (~2.0 µs/row at 10M, ~1.7 µs/row at 100M).

---

## Phase 0 - Baseline (2026-04-17)

**Code:** pre-shredding baseline (branch prior to PR [#14297](https://github.com/apache/iceberg/pull/14297)). JSON vs unshredded variant only. No shredding writer in apache/main yet at this point in time. No `events_shredded` table.

**Operations:** `write-json`, `write-variant`, `read-baseline`, `read-{project,nested,filter,agg}-{json,variant}` (11 total).

### Small (10M rows)

**Writes (median ms):**

| Operation | ms | vs JSON |
|-----------|---:|--------:|
| write-json | 2,389 | 1.0x |
| write-variant | 7,971 | 3.3x |

**Reads (median ms):**

| Query Pattern | JSON | Variant | Variant vs JSON |
|---|---:|---:|---:|
| read-baseline (typed BIGINT) | 152 | - | - |
| Projection (flat) | 2,965 | 2,273 | 1.30x faster |
| Projection (nested) | 3,029 | 2,564 | 1.18x faster |
| Filter | 3,565 | 2,613 | 1.36x faster |
| Aggregation | 5,454 | 3,162 | 1.73x faster |

### Medium (100M rows)

**Writes (median ms):**

| Operation | ms | vs JSON |
|-----------|---:|--------:|
| write-json | 17,345 | 1.0x |
| write-variant | 72,501 | 4.2x |

**Reads (median ms):**

| Query Pattern | JSON | Variant | Variant vs JSON |
|---|---:|---:|---:|
| read-baseline (typed BIGINT) | 1,324 | - | - |
| Projection (flat) | 28,686 | 22,522 | 1.27x faster |
| Projection (nested) | 27,414 | 24,556 | 1.12x faster |
| Filter | 31,610 | 23,753 | 1.33x faster |
| Aggregation | 49,455 | 29,030 | 1.70x faster |
