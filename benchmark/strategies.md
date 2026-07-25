# Variant Inference Strategies

Companion to `benchmark/README.md` (harness usage) and `benchmark/REPORT.md` (run history). This document is self-contained: it describes the strategies compared, the workloads, the methodology, and the results that drove the recommendation.

## What is a shredding strategy

When `write.parquet.shred-variants=true`, the writer buffers rows and decides which paths inside each Variant column to extract as typed Parquet columns (`typed_value`). Everything not extracted stays in the residual `value`. The inference strategy is the rule that decides which paths and what type.

The strategy is invisible to readers: each file is self-describing and reconstructs correctly regardless of which strategy wrote it. Strategy choice only affects the writer's output shape (which typed columns get emitted), which affects downstream read cost, file size, and predicate pushdown.

## Method

Each cell writes an Iceberg table with a Variant column under a chosen inference strategy and buffer size, then reads it back. Environment: macOS on arm64; OpenJDK 17; Spark 4.1 in `local[4]` mode (single driver JVM, 4 executor threads); Iceberg built off `apache/main` with format-version 3; Parquet writer with zstd compression at default row-group and page sizes; data on local SSD.

Single-machine local runs. Absolute wall-clock is indicative and not comparable across hardware; per-cell ratios and relative rankings across strategies are the intended output.

Reader-only extraction: reads the shredded `typed_value` column directly out of Parquet, iterates rows, discards. No Spark, no variant reconstruction. Compared against a reconstruction path that goes through Spark and rebuilds the Variant into `InternalRow`. 5 iterations per cell, median wall-clock reported.

Per-column scoring: each shredded field is examined in the Parquet footer for row counts in the `typed_value` column vs the fallback `value` column.

### Scoring rule

Each shredded column scores:

- **+1** shredded, zero rows fall back to the untyped `value`, statistics present in the footer.
- **0** shredded, fewer than 10% of rows fall back.
- **-1** shredded, 10% or more of rows fall back.

A field that is not shredded does not contribute to the sum. Per-workload score is the sum across all shredded columns.

### Correctness checks

Every cell must pass:

- **Row-preservation ratio = 1.0.** Row count from the strategy equals row count from the unshredded control. No rows lost.
- **Aggregate-fidelity delta (null-count) = 0.** The count of null vs non-null values per field matches between the shredded and unshredded reads. SUM correctness follows from null-count equality. MIN/MAX/DISTINCT_COUNT correctness requires query-time evaluation and is tracked as follow-up.
- **Scorer footer check = 0.** Null-row counts in the Parquet footer match between the `typed_value` column and its `value` sibling. Catches cases where the writer said a row was shredded but the reader would see a mismatch.

All 66 cells in the run pass all three.

## Workloads

Each workload is 100k rows, one Variant column, generated deterministically to stress a specific inference decision.

| # | ID | Description |
|---|---|---|
| 1 | w1-uniform | Every row has the same fields with the same scalar types. Baseline. |
| 2 | w2-uuid | Single high-cardinality string field per row. |
| 3 | w3-60-40 | A `duration` field is int in 60% of rows, string in 40%. |
| 4 | w3-95-5 | A `duration` field is int in 95% of rows, string in 5%. |
| 5 | w3-99-1 | A `duration` field is int in 99% of rows, string in 1%. |
| 6 | w4-long-array | 5% of rows carry a 1000-element nested array. |
| 7 | w5-clustered | Real GitHub Archive events, 24 hours, ~12 heterogeneous event schemas. The only production-adjacent workload. |
| 8 | w8-wide-object | 200 top-level keys per row; half present per row on average. |
| 9 | w9-blob-payload | 10-50 KB text blob per row alongside scalars. |
| 10 | w10-polymorphic | A `data` field's type varies row-to-row across four shapes (object, array, int, string in equal proportion). |
| 11 | w11-deeply-nested | A `config` field with 8 levels of nested objects. |

The `#` column is the workload's position in the study (1 through 11). Score tables below are split into positions 1-5 and 6-11. The `w`-prefix is a stable per-workload ID; `w6` and `w7` are gaps reserved during design and not built for this run.

## The strategies evaluated

Five inference strategies plus an unshredded control. All shredded strategies use the same 100-row buffer and the same numeric widening (INT8-INT64 collapse to widest observed; DECIMAL4-DECIMAL16 collapse to widest precision/scale). They differ in the admission rule.

| Name | Definition |
|---|---|
| `unshredded` | Baseline. `write.parquet.shred-variants=false`. No `typed_value` columns. |
| `v1-majority` | apache/main default (PR #14297). Admits any field observed in >= 10% of buffered rows; picks the winning type by `argmax(count)` with `TIE_BREAK_PRIORITY` breaking ties. Minority-type rows fall back to `value`. |
| `v2-uniform` | Layered on `v1-majority`. Adds a strict type-uniformity check: shred only when all sampled rows agree on the type after numeric widening. If any minority type slips through, don't shred. |
| `v2-first-20-uniform` | Same strict uniformity check as `v2-uniform`, but on a fixed 20-row sample instead of the full buffer. |
| `v2-uniform-wilson (sample=100)` | Layered on `v2-uniform`. Relaxes the strict check statistically: admit shredding when the Wilson 95% lower bound of the type-agreement rate is >= 0.99. |
| `v2-uniform-wilson (sample=1000)` | Same relaxation at a larger sample size. |

Total: 6 cells per workload, 11 workloads, 66 cells.

## Algorithm selection

**Question:** which inference algorithm is the default when shredding is enabled?

**Per-column score, workloads 1-5:**

| Strategy | w1-uniform | w2-uuid | w3-60-40 | w3-95-5 | w3-99-1 |
|---|---|---|---|---|---|
| unshredded | 0 | 0 | 0 | 0 | 0 |
| v1-majority | 6 | 1 | 6 | 6 | 6 |
| **v2-uniform** | **6** | **1** | **7** | **7** | **7** |
| v2-first-20-uniform | 6 | 1 | 7 | 7 | 6 |
| wilson (sample 100) | 6 | 1 | 7 | 7 | 6 |
| wilson (sample 1000) | 6 | 1 | 7 | 6 | 6 |

**Per-column score, workloads 6-11:**

| Strategy | w4-long-array | w5-clustered | w8-wide-object | w9-blob-payload | w10-polymorphic | w11-deeply-nested |
|---|---|---|---|---|---|---|
| unshredded | 0 | 0 | 0 | 0 | 0 | 0 |
| v1-majority | 6 | 56 | 6 | 5 | 5 | 6 |
| **v2-uniform** | **6** | **56** | **6** | **5** | **6** | **6** |
| v2-first-20-uniform | 6 | 53 | 6 | 5 | 6 | 6 |
| wilson (sample 100) | 6 | 51 | 6 | 5 | 6 | 6 |
| wilson (sample 1000) | 6 | 56 | 6 | 5 | 6 | 6 |

**Cross-strategy spread** (max minus min across the 5 shredded strategies per workload):

| Workload | Spread | Detail |
|---|---|---|
| w5-clustered | 5 | v1=56, v2=56, wilson-1000=56, v2-first-20=53, wilson-100=51 (range: 51-56) |
| w3-95-5 | 1 | three at 7, wilson-1000 at 6 |
| w3-60-40 | 1 | v1=6, others=7 |
| w3-99-1 | 1 | v2=7, others=6 |
| w10-polymorphic | 1 | v1=5, others=6 |
| w1-uniform, w2-uuid, w4-long-array, w8-wide-object, w9-blob-payload, w11-deeply-nested | 0 | all strategies agree |

On 6 of the 11 workloads all shredded strategies score identically (spread=0). `v2-uniform` wins uniquely on 4 workloads by 1 point; the recommendation rests on never regressing vs `v1-majority`, not on a decisive per-workload margin.

10 of 11 workloads land within 1 point across all strategies. Only `w5-clustered` (GHArchive event logs, 24 hours, roughly 12 event types with mixed schemas) shows meaningful spread.

### Decision rule

Recommend algorithm X when it satisfies all three:

- (a) per-column score >= `v1-majority` on all 11 workloads;
- (b) no cell scores -1 anywhere (no algorithm-caused stats destruction);
- (c) file size on shredded workloads within noise of the best strategy.

### Result: v2-uniform is the recommendation

- Scores >= `v1-majority` on **11 of 11** workloads.
- Wins uniquely by 1 point on **4 workloads** (w3-60-40, w3-95-5, w3-99-1, w10-polymorphic).
- No -1 cells anywhere in the matrix.
- File sizes match `v1-majority` within run-to-run noise on all shredded workloads.
- Does not lag on `w5-clustered`, unlike `v2-uniform-wilson` at sample=100 which is 5 points behind.

Wilson at sample=1000 matches `v2-uniform` on 10 of 11 workloads; the confidence-interval computation buys no different decision at this size.

## Reader-only extraction cost

**Question:** how much of the current shredded-read slowdown is attributable to inference choice vs the reader stack above?

Two paths on the same shredded Parquet fixture:

- **Typed-value path:** read the shredded `typed_value` column directly, iterate rows, discard. No Spark, no variant reconstruction.
- **Reconstruction path:** read through Spark, rebuild the variant, project the field, reserialize into `InternalRow`.

Isolating the typed-value path from Spark reconstruction lets us attribute today's shredded-read cost to the reader stack vs the inference layer. Production reads go through the reconstruction path; the isolation is not a replacement for it.

Reported ratio = (reconstruction time) / (typed-value time).

**Reconstruction and typed-value medians (ratio of medians):**

| Strategy | typed-value (ms) | reconstruction (ms) | Ratio |
|---|---|---|---|
| v1-majority | 5.88 | 322.78 | 54.94 |
| **v2-uniform** | **5.87** | **369.96** | **63.08** |
| v2-first-20-uniform | 5.75 | 355.82 | 61.94 |
| wilson (sample 100) | 5.83 | 369.10 | 63.36 |

Ratio here is (median reconstruction) / (median typed-value) computed on the two medians. Per-workload ratios are in the two tables below; the median of per-workload ratios is close but not identical to this number.

`v2-uniform`'s reconstruction median (370ms) is 14.5% higher than `v1-majority`'s (323ms). Spark reconstruction accounts for this overhead, not inference; the >50x typed-value speedup dominates.

**Per-workload ratios (workloads 1-4):**

| Workload | v1-majority | v2-uniform | v2-first-20-uniform | wilson (sample 100) |
|---|---|---|---|---|
| w1-uniform | 53.02 | 53.37 | 56.43 | 50.73 |
| w2-uuid | 6.30 | 6.10 | 5.90 | 5.08 |
| w3-60-40 | 54.91 | 64.01 | 65.56 | 67.17 |
| w3-95-5 | 50.30 | 64.89 | 63.01 | 53.20 |
| w3-99-1 | 52.17 | 59.97 | 54.92 | 53.35 |
| w4-long-array | 65.78 | 63.71 | 64.81 | 67.24 |

**Per-workload ratios (workloads 5-11):**

| Workload | v1-majority | v2-uniform | v2-first-20-uniform | wilson (sample 100) |
|---|---|---|---|---|
| w5-clustered | 74.64 | 72.04 | 64.53 | 80.43 |
| w8-wide-object | 893.99 | 808.65 | 527.69 | 827.13 |
| w9-blob-payload | 153.91 | 149.44 | 120.84 | 140.54 |
| w10-polymorphic | 46.87 | 59.78 | 58.64 | 60.79 |
| w11-deeply-nested | 118.66 | 120.45 | 118.90 | 120.11 |

**Result:** typed-value read is 50-65x faster than Spark reconstruction at the median. Two outliers: `w2-uuid` at ~6x (single-column, Parquet I/O dominates); `w8-wide-object` at ~800x (reconstruction materializes 200 keys per row). Today's shredded read cost sits in the reconstruction layer above the reader, not in inference. Inference choice does not materially change this.

## Arrays workload

Separate 100k-row corpus: `id BIGINT`, `arr_text` (list of 256 strings), `arr_number` (list of 256 ints), `arr_graph` (64x64 int matrix as list of list of int).

Two reader queries measured:

- **arr_number[0]:** `SELECT variant_get(arr_number, '$[0]', 'int') FROM arrays_shredded;`
- **arr_text[0]:** `SELECT variant_get(arr_text, '$[0]', 'string') FROM arrays_shredded;`

**File layout, write time, and reader timings:**

| Cell | File size (MB) | Write (s) | arr_number[0] (ms) | arr_text[0] (ms) |
|---|---|---|---|---|
| unshredded | 1809 | 11 | 4.24 | 3.78 |
| v1-majority | 1571 | 15 | 21.10 | 38.74 |
| v2-uniform | 1571 | 15 | 19.46 | 38.16 |
| v2-first-20-uniform | 1571 | 16 | 18.53 | 38.20 |
| wilson (sample 100) | 1571 | 16 | 20.82 | 38.91 |
| wilson (sample 1000) | 1571 | 16 | 18.52 | 38.13 |

**Per-column scores:**

| Cell | arr_number | arr_text | arr_graph | Aggregate |
|---|---|---|---|---|
| v1-majority | 0 | +1 | 0 | +1 |
| v2-uniform | 0 | +1 | 0 | +1 |
| v2-first-20-uniform | 0 | +1 | 0 | +1 |
| wilson (sample 100) | 0 | +1 | 0 | +1 |
| wilson (sample 1000) | 0 | +1 | 0 | +1 |

- `arr_text`: 100% typed, no fallbacks.
- `arr_number`: about 3.27% of rows have at least one element that falls back to `value`. Variant binary encoding uses narrower int widths (INT8, INT16) that do not cleanly serialize into a 32-bit `typed_value` column.
- `arr_graph`: about 3.28% of rows fall back on the inner nested int leaf.

**Result:** all 5 shredded cells produce byte-identical fixtures; 13.2% smaller than unshredded; no strategy variance. Arrays are uniformly typed so strategy choice does not matter here.

Caveat: unshredded and shredded read paths measure different things (variant binary read vs typed element-index extraction); not directly comparable.

## Real-world coverage

Only `w5-clustered` (GHArchive) is production data. 10 workloads are synthetic. At least one additional real dataset is a candidate for a follow-up run before the recommendation goes to the wider community. Candidates: second GHArchive slice at a different date, TPC-DS variant column derivative.

Field evolution (new fields, type drift across writes) and schema-drift stability are not measured here. They require a `w12-schema-drift` workload that has not been built. Tracked as a follow-up.

## What NOT to conclude from wall-clock

- A strategy that refuses to shred (leaves everything in `value`) will match `unshredded` on writes and reads. That is not evidence the strategy is bad; it means the workload's shape did not admit any candidate.
- A strategy that shreds aggressively and pays high fallback writes will still show low read cost when queries do not project the shredded fields. Look at the Parquet footer fallback rate to catch this.
- File size differences are load-bearing: a strategy that shreds a wide field into a large `typed_value` column can trade write time for read time. Report both.

The row-preservation-ratio and aggregate-fidelity-delta gates protect against methodology bugs, not against strategy-quality claims. A strategy can pass both gates and still be a bad default. Use per-column fallback rates plus wall-clock together.

## Enabling a strategy

Three equivalent surfaces. Use the one that fits your workflow.

Per table (persists across writes):

```
ALTER TABLE events SET TBLPROPERTIES (
  'write.parquet.variant-inference-strategy' = 'v2-uniform'
);
```

Per Spark session (overrides the table default for writes in this session):

```
SET spark.sql.iceberg.variant-inference-strategy = v2-uniform;
```

Per DataFrame write:

```
df.writeTo("db.events")
  .option("variant-inference-strategy", "v2-uniform")
  .append();
```

The strategy applies only when `write.parquet.shred-variants=true`. With shredding disabled the property is ignored.

## Running the matrix

Full matrix (all strategies x all workloads at the default scale):

```
./benchmark/scripts/run-matrix.sh
```

Restrict to one strategy or one workload:

```
./benchmark/scripts/run-matrix.sh --strategy v2-uniform
./benchmark/scripts/run-matrix.sh --workload w3-60-40
```

Different scale (default `small`):

```
BENCH_SCALE=nano ./benchmark/scripts/run-matrix.sh
```

Bring-your-own staging dir (skips workload generation, uses an external corpus):

```
./benchmark/scripts/run-matrix.sh --staging-dir /path/to/data
```

Scoring and plotting downstream:

```
./benchmark/scripts/score-matrix.sh <matrix-run-dir>
./benchmark/scripts/plot-matrix.sh   <matrix-run-dir>
```

## References

- [Parquet Variant specification](https://github.com/apache/parquet-format/blob/main/VariantShredding.md) - `VariantEncoding.md` + `VariantShredding.md`.
- Iceberg [PR #14297](https://github.com/apache/iceberg/pull/14297) - Spark shredded write path.
- Iceberg issue [#10392](https://github.com/apache/iceberg/issues/10392) - Variant Data Type Support tracking.
