/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.benchmark;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.spark.sql.SparkSession;

/**
 * Benchmark driver for Spark. 2-column schema: event_id BIGINT, payload STRING/VARIANT.
 *
 * <p>Writes ARE the data loading - no hidden untimed INSERTs. Reads run against tables populated by
 * writes. If reads are requested but tables are empty, the benchmark fails with a clear error.
 *
 * <p>Fairness: both tables use format-version 3, same compression, vectorization disabled on both,
 * identical 2-column schema. Only difference: payload is STRING in events_json, VARIANT in
 * events_variant.
 */
public class SparkBenchDriver {

  private final String warehouse;
  private final String staging;
  private final String resultsBase;
  private final int threads;
  private final int warmup;
  private final int iterations;
  private final String compression;
  private final String inferenceStrategy;
  private final int bufferSize;
  private final List<String> operations;

  private SparkSession spark;
  private SparkMetricsListener metricsListener;

  private static final String COMMON_PROPS =
      "'format-version' = '3', "
          + "'write.format.default' = 'parquet', "
          + "'write.metadata.compression-codec' = 'gzip', "
          + "'read.parquet.vectorization.enabled' = 'false'";

  public SparkBenchDriver() {
    this.warehouse = System.getProperty("bench.warehouse", "/tmp/iceberg-bench/warehouse/spark");
    this.staging = System.getProperty("bench.staging", "/tmp/iceberg-bench/staging-v2-small");
    this.resultsBase = System.getProperty("bench.results", "/tmp/iceberg-bench/results/spark");
    this.threads = Integer.parseInt(System.getProperty("bench.threads", "4"));
    this.warmup = Integer.parseInt(System.getProperty("bench.warmup", "2"));
    this.iterations = Integer.parseInt(System.getProperty("bench.iterations", "5"));
    this.compression = System.getProperty("bench.compression", "zstd");
    this.inferenceStrategy = System.getProperty("bench.strategy", "b1-majority");
    this.bufferSize = Integer.parseInt(System.getProperty("bench.buffer-size", "100"));

    String ops = System.getProperty("bench.operations", "all");
    if ("all".equals(ops)) {
      this.operations =
          Arrays.asList(
              "write-json",
              "write-variant",
              "write-shredded",
              "write-shredded-buf10",
              "write-shredded-buf1000",
              "write-shredded-buf10000",
              "read-baseline",
              "read-project-json",
              "read-project-variant",
              "read-project-shredded",
              "read-project-shredded-buf10",
              "read-project-shredded-buf1000",
              "read-project-shredded-buf10000",
              "read-nested-json",
              "read-nested-variant",
              "read-nested-shredded",
              "read-filter-json",
              "read-filter-variant",
              "read-filter-shredded",
              "read-agg-json",
              "read-agg-variant",
              "read-agg-shredded");
    } else {
      this.operations = Arrays.asList(ops.split(","));
    }
  }

  private void initSpark() {
    spark =
        SparkSession.builder()
            .appName("IcebergBaselineBenchmark-Spark")
            .master("local[" + threads + "]")
            .config("spark.ui.enabled", "false")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.bench", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.bench.type", "hadoop")
            .config("spark.sql.catalog.bench.warehouse", "file:" + warehouse)
            .config("spark.sql.shuffle.partitions", String.valueOf(threads))
            .getOrCreate();
    metricsListener = new SparkMetricsListener();
    spark.sparkContext().addSparkListener(metricsListener);
  }

  private void createTables() {
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bench.db");

    // JSON table - only create if it doesn't exist (reads JVM reuses tables writes JVM created)
    spark.sql(
        String.format(
            Locale.ROOT,
            "CREATE TABLE IF NOT EXISTS bench.db.events_json ("
                + "event_id BIGINT, payload STRING"
                + ") USING iceberg TBLPROPERTIES (%s, "
                + "'write.parquet.compression-codec' = '%s')",
            COMMON_PROPS,
            compression));

    // Variant table
    spark.sql(
        String.format(
            Locale.ROOT,
            "CREATE TABLE IF NOT EXISTS bench.db.events_variant ("
                + "event_id BIGINT, payload VARIANT"
                + ") USING iceberg TBLPROPERTIES (%s, "
                + "'write.parquet.compression-codec' = '%s')",
            COMMON_PROPS,
            compression));

    // Shredded variant table - relies on PR #14297 in apache/main
    spark.sql(
        String.format(
            Locale.ROOT,
            "CREATE TABLE IF NOT EXISTS bench.db.events_shredded ("
                + "event_id BIGINT, payload VARIANT"
                + ") USING iceberg TBLPROPERTIES (%s, "
                + "'write.parquet.compression-codec' = '%s', "
                + "'write.parquet.shred-variants' = 'true', "
                + "'write.parquet.variant-inference-strategy' = '%s', "
                + "'write.parquet.variant-inference-buffer-size' = '%d')",
            COMMON_PROPS,
            compression,
            inferenceStrategy,
            bufferSize));

    // Sensitivity tables: vary write.parquet.variant-inference-buffer-size (default is 100,
    // which the events_shredded table above uses). MIN_FIELD_FREQUENCY=0.10 in
    // VariantShreddingAnalyzer is applied against this buffer, so smaller buffers shred
    // aggressively (a single occurrence is enough) and larger buffers shred conservatively.
    for (int bufferSize : new int[] {10, 1000, 10000}) {
      spark.sql(
          String.format(
              Locale.ROOT,
              "CREATE TABLE IF NOT EXISTS bench.db.events_shredded_buf%d ("
                  + "event_id BIGINT, payload VARIANT"
                  + ") USING iceberg TBLPROPERTIES (%s, "
                  + "'write.parquet.compression-codec' = '%s', "
                  + "'write.parquet.shred-variants' = 'true', "
                  + "'write.parquet.variant-inference-buffer-size' = '%d', "
                  + "'write.parquet.variant-inference-strategy' = '%s')",
              bufferSize,
              COMMON_PROPS,
              compression,
              bufferSize,
              inferenceStrategy));
    }

    // arrays_variant family: Snowflake Q6-Q11 parity tables. Columns are stored as VARIANT
    // (raw), VARIANT (shredded), and JSON STRING respectively. Source schema lives in
    // DataGenerator.SCHEMA_ARRAYS; writes register the staged_arrays temp view below.
    spark.sql(
        String.format(
            Locale.ROOT,
            "CREATE TABLE IF NOT EXISTS bench.db.arrays_json ("
                + "id BIGINT, arr_text STRING, arr_number STRING, arr_graph STRING"
                + ") USING iceberg TBLPROPERTIES (%s, "
                + "'write.parquet.compression-codec' = '%s')",
            COMMON_PROPS,
            compression));

    spark.sql(
        String.format(
            Locale.ROOT,
            "CREATE TABLE IF NOT EXISTS bench.db.arrays_variant ("
                + "id BIGINT, arr_text VARIANT, arr_number VARIANT, arr_graph VARIANT"
                + ") USING iceberg TBLPROPERTIES (%s, "
                + "'write.parquet.compression-codec' = '%s')",
            COMMON_PROPS,
            compression));

    spark.sql(
        String.format(
            Locale.ROOT,
            "CREATE TABLE IF NOT EXISTS bench.db.arrays_shredded ("
                + "id BIGINT, arr_text VARIANT, arr_number VARIANT, arr_graph VARIANT"
                + ") USING iceberg TBLPROPERTIES (%s, "
                + "'write.parquet.compression-codec' = '%s', "
                + "'write.parquet.shred-variants' = 'true', "
                + "'write.parquet.variant-inference-strategy' = '%s')",
            COMMON_PROPS,
            compression,
            inferenceStrategy));

    // Register staging data as temp view
    spark.read().parquet(staging + "/events").createOrReplaceTempView("staged_events");

    // arrays staging is a SEPARATE dataset, not collocated with each workload's events. It lives
    // at <scale>/arrays/events/ (sibling of <scale>/<workload>/events/), and can be overridden via
    // -Dbench.arrays-staging=<path>. Register the temp view only when the data is present so
    // events-only runs keep working.
    String arraysStagingProp = System.getProperty("bench.arrays-staging");
    String arraysStaging;
    if (arraysStagingProp != null && !arraysStagingProp.isEmpty()) {
      arraysStaging = arraysStagingProp;
    } else {
      java.io.File workloadDir = new java.io.File(staging);
      java.io.File scaleDir = workloadDir.getParentFile();
      arraysStaging =
          scaleDir == null ? null : new java.io.File(scaleDir, "arrays").getAbsolutePath();
    }
    if (arraysStaging != null) {
      java.io.File arraysEvents = new java.io.File(arraysStaging, "events");
      if (arraysEvents.isDirectory()
          && arraysEvents.listFiles((dir, name) -> name.endsWith(".parquet")) != null
          && arraysEvents.listFiles((dir, name) -> name.endsWith(".parquet")).length > 0) {
        spark.read().parquet(arraysStaging + "/events").createOrReplaceTempView("staged_arrays");
        System.out.println("Registered staged_arrays temp view from " + arraysStaging + "/events");
      } else {
        System.out.println(
            "No arrays staging at "
                + arraysStaging
                + "/events - arrays write ops + sf-q6..q11 will be skipped");
      }
    }
  }

  private List<Long> runOperation(String name, Runnable operation) {
    System.out.println("--- Running: " + name + " ---");

    for (int i = 0; i < warmup; i++) {
      System.out.printf(Locale.ROOT, "  Warmup %d/%d%n", i + 1, warmup);
      operation.run();
    }

    List<Long> timings = new ArrayList<>();
    List<SparkMetricsListener.SparkMetricsSummary> perIterMetrics = new ArrayList<>();

    for (int i = 0; i < iterations; i++) {
      // Drain listener bus + reset counters so the iteration's metrics are clean.
      waitForListenerBus();
      metricsListener.reset();
      long start = System.nanoTime();
      operation.run();
      long elapsed = System.nanoTime() - start;
      // Wait for any tail TaskEnd events from the just-run query before snapshotting.
      waitForListenerBus();
      perIterMetrics.add(metricsListener.snapshot());
      timings.add(elapsed);
      System.out.printf(
          Locale.ROOT, "  Iteration %d/%d: %d ms%n", i + 1, iterations, elapsed / 1_000_000);
    }

    Path outputDir = Paths.get(resultsBase, name, "parquet-" + compression);
    try {
      BenchmarkResultWriter.writeTimings(outputDir, name, timings);
      BenchmarkResultWriter.writeSparkMetrics(outputDir, name, perIterMetrics);
      // Note: scan/commit reports are not captured because the catalog creates its own
      // MetricsReporter instance via reflection, separate from the driver's instance.
      // Timing data is the primary metric. Iceberg metrics require catalog-level integration.
    } catch (IOException e) {
      System.err.println("Failed to write results for " + name + ": " + e.getMessage());
    }

    long median = timings.stream().sorted().skip(timings.size() / 2).findFirst().orElse(0L);
    System.out.printf(Locale.ROOT, "  Median: %d ms%n%n", median / 1_000_000);
    return timings;
  }

  // Drain the listener bus so engine-metric snapshots reflect only the iteration we just ran.
  // Timeout means a TaskEnd event is still in flight; we log and continue rather than failing
  // the benchmark, since the only consequence is one iteration's metrics may undercount.
  private void waitForListenerBus() {
    try {
      spark.sparkContext().listenerBus().waitUntilEmpty(10_000);
    } catch (java.util.concurrent.TimeoutException e) {
      System.err.println("  WARN: listener bus did not drain within 10s: " + e.getMessage());
    }
  }

  private void captureFileSize(String operation, String tableName) {
    try {
      org.apache.spark.sql.Row stats =
          spark
              .sql(
                  String.format(
                      Locale.ROOT,
                      "SELECT COALESCE(SUM(file_size_in_bytes), 0) AS bytes, COUNT(*) AS files FROM %s.files",
                      tableName))
              .first();
      long totalBytes = stats.getLong(0);
      int fileCount = (int) stats.getLong(1);
      Path cellDir = Paths.get(resultsBase);
      BenchmarkResultWriter.writeFileSize(cellDir, operation, totalBytes, fileCount);
      System.out.printf(
          Locale.ROOT, "  File size: %d bytes across %d files%n", totalBytes, fileCount);
    } catch (Exception e) {
      System.err.println("Failed to capture file size for " + operation + ": " + e.getMessage());
    }
  }

  /**
   * Run verification queries (untimed) for each filter/agg read op and write per-op (count, sum) to
   * correctness.json. The scorer joins this against the unshredded baseline cell for the same
   * workload to compute Row Preservation Ratio (catches B1's silent drop on mixed types).
   */
  private void captureCorrectness() {
    java.util.Map<String, long[]> verification = new java.util.LinkedHashMap<>();
    for (String op : operations) {
      if (!op.startsWith("read-filter-") && !op.startsWith("read-agg-") && !op.startsWith("sf-q")) {
        continue;
      }
      String query = verifyQueryFor(op);
      if (query == null) {
        continue;
      }
      try {
        org.apache.spark.sql.Row row = spark.sql(query).first();
        long count = row.isNullAt(0) ? 0L : row.getLong(0);
        long sum = (row.size() > 1 && !row.isNullAt(1)) ? row.getLong(1) : 0L;
        verification.put(op, new long[] {count, sum});
      } catch (Exception e) {
        System.err.println("Verification query failed for " + op + ": " + e.getMessage());
      }
    }
    if (verification.isEmpty()) {
      return;
    }
    try {
      BenchmarkResultWriter.writeCorrectness(Paths.get(resultsBase), verification);
      System.out.printf(Locale.ROOT, "  Correctness: %d ops verified%n", verification.size());
    } catch (IOException e) {
      System.err.println("Failed to write correctness.json: " + e.getMessage());
    }
  }

  /**
   * Returns the verification query for a read op, or null if the op has no row-count semantics
   * (e.g., aggregations that group-by produce row sets, not single counts). Aggregation ops are
   * still verified by collapsing to a global SUM across all groups.
   *
   * <p>Verification queries deliberately mirror the timed ops so a divergence in result indicates a
   * correctness regression in shredded reading, not a query difference.
   */
  private String verifyQueryFor(String op) {
    switch (op) {
      case "read-filter-json":
        return "SELECT COUNT(event_id), SUM(CAST(get_json_object(payload, '$.duration') AS BIGINT)) FROM bench.db.events_json WHERE get_json_object(payload, '$.event_type') = 'purchase' AND get_json_object(payload, '$.country') = 'US'";
      case "read-filter-variant":
        return "SELECT COUNT(event_id), SUM(CAST(try_variant_get(payload, '$.duration', 'int') AS BIGINT)) FROM bench.db.events_variant WHERE try_variant_get(payload, '$.event_type', 'string') = 'purchase' AND try_variant_get(payload, '$.country', 'string') = 'US'";
      case "read-filter-shredded":
        return "SELECT COUNT(event_id), SUM(CAST(try_variant_get(payload, '$.duration', 'int') AS BIGINT)) FROM bench.db.events_shredded WHERE try_variant_get(payload, '$.event_type', 'string') = 'purchase' AND try_variant_get(payload, '$.country', 'string') = 'US'";
      case "read-agg-json":
        return "SELECT COUNT(event_id), SUM(CAST(get_json_object(payload, '$.duration') AS BIGINT)) FROM bench.db.events_json";
      case "read-agg-variant":
        return "SELECT COUNT(event_id), SUM(CAST(try_variant_get(payload, '$.duration', 'int') AS BIGINT)) FROM bench.db.events_variant";
      case "read-agg-shredded":
        return "SELECT COUNT(event_id), SUM(CAST(try_variant_get(payload, '$.duration', 'int') AS BIGINT)) FROM bench.db.events_shredded";
        // -- Snowflake Q1: top-level field aggregation. Single (count_rows, sum_duration). --
      case "sf-q1-variant":
        return "SELECT COUNT(*), SUM(CAST(try_variant_get(payload, '$.duration', 'int') AS BIGINT)) FROM bench.db.events_variant";
      case "sf-q1-shredded":
        return "SELECT COUNT(*), SUM(CAST(try_variant_get(payload, '$.duration', 'int') AS BIGINT)) FROM bench.db.events_shredded";
        // -- Snowflake Q2: deeply nested path. (count_rows, sum_load_ms). --
      case "sf-q2-variant":
        return "SELECT COUNT(*), SUM(CAST(try_variant_get(payload, '$.metrics.timing.load_ms', 'int') AS BIGINT)) FROM bench.db.events_variant";
      case "sf-q2-shredded":
        return "SELECT COUNT(*), SUM(CAST(try_variant_get(payload, '$.metrics.timing.load_ms', 'int') AS BIGINT)) FROM bench.db.events_shredded";
        // -- Snowflake Q3: filtered aggregation. (count_passing, sum_duration). --
      case "sf-q3-variant":
        return "SELECT COUNT(event_id), SUM(CAST(try_variant_get(payload, '$.duration', 'int') AS BIGINT)) FROM bench.db.events_variant WHERE try_variant_get(payload, '$.event_type', 'string') = 'purchase' AND try_variant_get(payload, '$.country', 'string') = 'US'";
      case "sf-q3-shredded":
        return "SELECT COUNT(event_id), SUM(CAST(try_variant_get(payload, '$.duration', 'int') AS BIGINT)) FROM bench.db.events_shredded WHERE try_variant_get(payload, '$.event_type', 'string') = 'purchase' AND try_variant_get(payload, '$.country', 'string') = 'US'";
        // -- Snowflake Q4: GROUP BY rollup. Verify by collapsing groups: (distinct_event_types,
        // total_duration_across_all). --
      case "sf-q4-variant":
        return "SELECT COUNT(DISTINCT try_variant_get(payload, '$.event_type', 'string')), SUM(CAST(try_variant_get(payload, '$.duration', 'int') AS BIGINT)) FROM bench.db.events_variant";
      case "sf-q4-shredded":
        return "SELECT COUNT(DISTINCT try_variant_get(payload, '$.event_type', 'string')), SUM(CAST(try_variant_get(payload, '$.duration', 'int') AS BIGINT)) FROM bench.db.events_shredded";
        // -- Snowflake Q5: TOP-10 by duration. Verify by computing row count + sum_duration of the
        // same TOP-10. --
      case "sf-q5-variant":
        return "SELECT COUNT(*), SUM(CAST(try_variant_get(payload, '$.duration', 'int') AS BIGINT)) FROM (SELECT payload FROM bench.db.events_variant ORDER BY try_variant_get(payload, '$.duration', 'int') LIMIT 10)";
      case "sf-q5-shredded":
        return "SELECT COUNT(*), SUM(CAST(try_variant_get(payload, '$.duration', 'int') AS BIGINT)) FROM (SELECT payload FROM bench.db.events_shredded ORDER BY try_variant_get(payload, '$.duration', 'int') LIMIT 10)";
        // -- Snowflake Q6: numeric element access on arrays_*. (count_rows, sum_first_element). --
      case "sf-q6-variant":
        return "SELECT COUNT(*), SUM(CAST(try_variant_get(arr_number, '$[0]', 'int') AS BIGINT)) FROM bench.db.arrays_variant";
      case "sf-q6-shredded":
        return "SELECT COUNT(*), SUM(CAST(try_variant_get(arr_number, '$[0]', 'int') AS BIGINT)) FROM bench.db.arrays_shredded";
        // -- Snowflake Q7: string element access. MIN is not summable; verify via COUNT only,
        // plus a hash to detect divergence between variant and shredded. --
      case "sf-q7-variant":
        return "SELECT COUNT(*), SUM(CAST(HASH(try_variant_get(arr_text, '$[0]', 'string')) AS BIGINT)) FROM bench.db.arrays_variant";
      case "sf-q7-shredded":
        return "SELECT COUNT(*), SUM(CAST(HASH(try_variant_get(arr_text, '$[0]', 'string')) AS BIGINT)) FROM bench.db.arrays_shredded";
        // -- Snowflake Q8: nested 2D access. (count_rows, sum_at_0_3). --
      case "sf-q8-variant":
        return "SELECT COUNT(*), SUM(CAST(try_variant_get(arr_graph, '$[0][3]', 'int') AS BIGINT)) FROM bench.db.arrays_variant";
      case "sf-q8-shredded":
        return "SELECT COUNT(*), SUM(CAST(try_variant_get(arr_graph, '$[0][3]', 'int') AS BIGINT)) FROM bench.db.arrays_shredded";
        // -- Snowflake Q9/Q10/Q11: TOP-10 retrievals. Verify by recomputing count + sum of the
        // sort key on the same TOP-10 set. --
      case "sf-q9-variant":
        return "SELECT COUNT(*), SUM(CAST(try_variant_get(arr_number, '$[0]', 'int') AS BIGINT)) FROM (SELECT arr_number FROM bench.db.arrays_variant ORDER BY try_variant_get(arr_number, '$[0]', 'int') LIMIT 10)";
      case "sf-q9-shredded":
        return "SELECT COUNT(*), SUM(CAST(try_variant_get(arr_number, '$[0]', 'int') AS BIGINT)) FROM (SELECT arr_number FROM bench.db.arrays_shredded ORDER BY try_variant_get(arr_number, '$[0]', 'int') LIMIT 10)";
      case "sf-q10-variant":
        return "SELECT COUNT(*), SUM(CAST(HASH(try_variant_get(arr_text, '$[0]', 'string')) AS BIGINT)) FROM (SELECT arr_text FROM bench.db.arrays_variant ORDER BY try_variant_get(arr_text, '$[0]', 'string') LIMIT 10)";
      case "sf-q10-shredded":
        return "SELECT COUNT(*), SUM(CAST(HASH(try_variant_get(arr_text, '$[0]', 'string')) AS BIGINT)) FROM (SELECT arr_text FROM bench.db.arrays_shredded ORDER BY try_variant_get(arr_text, '$[0]', 'string') LIMIT 10)";
      case "sf-q11-variant":
        return "SELECT COUNT(*), SUM(CAST(try_variant_get(arr_graph, '$[0][3]', 'int') AS BIGINT)) FROM (SELECT arr_graph FROM bench.db.arrays_variant ORDER BY try_variant_get(arr_graph, '$[0][3]', 'int') LIMIT 10)";
      case "sf-q11-shredded":
        return "SELECT COUNT(*), SUM(CAST(try_variant_get(arr_graph, '$[0][3]', 'int') AS BIGINT)) FROM (SELECT arr_graph FROM bench.db.arrays_shredded ORDER BY try_variant_get(arr_graph, '$[0][3]', 'int') LIMIT 10)";
      default:
        return null;
    }
  }

  /**
   * Returns true if the op targets the arrays_* table family (Snowflake Q6-Q11), so the read path
   * knows to verify arrays_variant / arrays_shredded rather than events_variant / events_shredded.
   */
  private static boolean isArraysOp(String op) {
    return op.startsWith("sf-q6-")
        || op.startsWith("sf-q7-")
        || op.startsWith("sf-q8-")
        || op.startsWith("sf-q9-")
        || op.startsWith("sf-q10-")
        || op.startsWith("sf-q11-");
  }

  // ---------------------------------------------------------------------------
  // Writes - these ARE the data loading. No hidden untimed work.
  // For multiple iterations: drop + recreate between iterations.
  // Last iteration's table persists for reads.
  // ---------------------------------------------------------------------------

  private void runWriteBenchmarks() {
    if (operations.contains("write-json")) {
      runOperation(
          "write-json",
          () -> {
            spark.sql("DROP TABLE IF EXISTS bench.db.events_json");
            spark.sql(
                String.format(
                    Locale.ROOT,
                    "CREATE TABLE bench.db.events_json ("
                        + "event_id BIGINT, payload STRING"
                        + ") USING iceberg TBLPROPERTIES (%s, "
                        + "'write.parquet.compression-codec' = '%s')",
                    COMMON_PROPS,
                    compression));
            spark.sql(
                "INSERT INTO bench.db.events_json SELECT event_id, payload FROM staged_events");
          });
      captureFileSize("write-json", "bench.db.events_json");
    }

    if (operations.contains("write-variant")) {
      runOperation(
          "write-variant",
          () -> {
            spark.sql("DROP TABLE IF EXISTS bench.db.events_variant");
            spark.sql(
                String.format(
                    Locale.ROOT,
                    "CREATE TABLE bench.db.events_variant ("
                        + "event_id BIGINT, payload VARIANT"
                        + ") USING iceberg TBLPROPERTIES (%s, "
                        + "'write.parquet.compression-codec' = '%s')",
                    COMMON_PROPS,
                    compression));
            spark.sql(
                "INSERT INTO bench.db.events_variant "
                    + "SELECT event_id, parse_json(payload) FROM staged_events");
          });
      captureFileSize("write-variant", "bench.db.events_variant");
    }

    if (operations.contains("write-shredded")) {
      runOperation(
          "write-shredded",
          () -> {
            spark.sql("DROP TABLE IF EXISTS bench.db.events_shredded");
            spark.sql(
                String.format(
                    Locale.ROOT,
                    "CREATE TABLE bench.db.events_shredded ("
                        + "event_id BIGINT, payload VARIANT"
                        + ") USING iceberg TBLPROPERTIES (%s, "
                        + "'write.parquet.compression-codec' = '%s', "
                        + "'write.parquet.shred-variants' = 'true', "
                        + "'write.parquet.variant-inference-strategy' = '%s', "
                        + "'write.parquet.variant-inference-buffer-size' = '%d')",
                    COMMON_PROPS,
                    compression,
                    inferenceStrategy,
                    bufferSize));
            System.err.printf(
                Locale.ROOT,
                "[BENCH] events_shredded created with variant-inference-buffer-size=%d strategy=%s%n",
                bufferSize,
                inferenceStrategy);
            spark.sql(
                "INSERT INTO bench.db.events_shredded "
                    + "SELECT event_id, parse_json(payload) FROM staged_events");
          });
      captureFileSize("write-shredded", "bench.db.events_shredded");
    }

    // Sensitivity sweep: vary inference-buffer-size on the same payload
    for (int bufferSize : new int[] {10, 1000, 10000}) {
      String opName = "write-shredded-buf" + bufferSize;
      if (operations.contains(opName)) {
        runOperation(
            opName,
            () -> {
              String tableName = "bench.db.events_shredded_buf" + bufferSize;
              spark.sql("DROP TABLE IF EXISTS " + tableName);
              spark.sql(
                  String.format(
                      Locale.ROOT,
                      "CREATE TABLE %s ("
                          + "event_id BIGINT, payload VARIANT"
                          + ") USING iceberg TBLPROPERTIES (%s, "
                          + "'write.parquet.compression-codec' = '%s', "
                          + "'write.parquet.shred-variants' = 'true', "
                          + "'write.parquet.variant-inference-buffer-size' = '%d', "
                          + "'write.parquet.variant-inference-strategy' = '%s')",
                      tableName,
                      COMMON_PROPS,
                      compression,
                      bufferSize,
                      inferenceStrategy));
              spark.sql(
                  "INSERT INTO "
                      + tableName
                      + " SELECT event_id, parse_json(payload) FROM staged_events");
            });
      }
    }

    // arrays_variant family writes. Mirror the events_* pattern: drop, recreate with the same
    // TBLPROPERTIES as createTables(), insert from staged_arrays. The JSON variant stores the
    // array columns as serialized JSON strings (parity with the JSON-string events table); the
    // VARIANT variants round-trip the typed arrays through parse_json(to_json(...)) so the
    // result is a VARIANT value that mirrors the Snowflake table shape.
    //
    // Skip ALL arrays ops if the staged_arrays temp view was not registered (no arrays staging
    // present). Logged in createTables() above; here we silently no-op to keep the matrix flowing
    // for events-only runs.
    boolean hasArraysStaging =
        !spark.catalog().listTables().filter("name = 'staged_arrays'").isEmpty();
    if (!hasArraysStaging) {
      if (operations.contains("write-arrays-json")
          || operations.contains("write-arrays-variant")
          || operations.contains("write-arrays-shredded")) {
        System.out.println(
            "Skipping arrays write ops: staged_arrays temp view not registered "
                + "(no arrays staging present for this run).");
      }
      return;
    }

    if (operations.contains("write-arrays-json")) {
      runOperation(
          "write-arrays-json",
          () -> {
            spark.sql("DROP TABLE IF EXISTS bench.db.arrays_json");
            spark.sql(
                String.format(
                    Locale.ROOT,
                    "CREATE TABLE bench.db.arrays_json ("
                        + "id BIGINT, arr_text STRING, arr_number STRING, arr_graph STRING"
                        + ") USING iceberg TBLPROPERTIES (%s, "
                        + "'write.parquet.compression-codec' = '%s')",
                    COMMON_PROPS,
                    compression));
            spark.sql(
                "INSERT INTO bench.db.arrays_json "
                    + "SELECT id, to_json(arr_text), to_json(arr_number), to_json(arr_graph) "
                    + "FROM staged_arrays");
          });
      captureFileSize("write-arrays-json", "bench.db.arrays_json");
    }

    if (operations.contains("write-arrays-variant")) {
      runOperation(
          "write-arrays-variant",
          () -> {
            spark.sql("DROP TABLE IF EXISTS bench.db.arrays_variant");
            spark.sql(
                String.format(
                    Locale.ROOT,
                    "CREATE TABLE bench.db.arrays_variant ("
                        + "id BIGINT, arr_text VARIANT, arr_number VARIANT, arr_graph VARIANT"
                        + ") USING iceberg TBLPROPERTIES (%s, "
                        + "'write.parquet.compression-codec' = '%s')",
                    COMMON_PROPS,
                    compression));
            spark.sql(
                "INSERT INTO bench.db.arrays_variant "
                    + "SELECT id, parse_json(to_json(arr_text)), "
                    + "parse_json(to_json(arr_number)), parse_json(to_json(arr_graph)) "
                    + "FROM staged_arrays");
          });
      captureFileSize("write-arrays-variant", "bench.db.arrays_variant");
    }

    if (operations.contains("write-arrays-shredded")) {
      runOperation(
          "write-arrays-shredded",
          () -> {
            spark.sql("DROP TABLE IF EXISTS bench.db.arrays_shredded");
            spark.sql(
                String.format(
                    Locale.ROOT,
                    "CREATE TABLE bench.db.arrays_shredded ("
                        + "id BIGINT, arr_text VARIANT, arr_number VARIANT, arr_graph VARIANT"
                        + ") USING iceberg TBLPROPERTIES (%s, "
                        + "'write.parquet.compression-codec' = '%s', "
                        + "'write.parquet.shred-variants' = 'true', "
                        + "'write.parquet.variant-inference-strategy' = '%s', "
                        + "'write.parquet.variant-inference-buffer-size' = '%d')",
                    COMMON_PROPS,
                    compression,
                    inferenceStrategy,
                    bufferSize));
            System.err.printf(
                Locale.ROOT,
                "[BENCH] arrays_shredded created with variant-inference-buffer-size=%d strategy=%s%n",
                bufferSize,
                inferenceStrategy);
            spark.sql(
                "INSERT INTO bench.db.arrays_shredded "
                    + "SELECT id, parse_json(to_json(arr_text)), "
                    + "parse_json(to_json(arr_number)), parse_json(to_json(arr_graph)) "
                    + "FROM staged_arrays");
          });
      captureFileSize("write-arrays-shredded", "bench.db.arrays_shredded");
    }
  }

  // ---------------------------------------------------------------------------
  // Reads - query tables populated by writes.
  // Fail if tables are empty (writes must run first).
  // ---------------------------------------------------------------------------

  private void runReadBenchmarks() {
    boolean hasAnyRead =
        operations.stream().anyMatch(op -> op.startsWith("read-") || op.startsWith("sf-q"));
    if (!hasAnyRead) {
      return;
    }

    // Events-* ops: standard events_json/events_variant/events_shredded suffix matching.
    // Arrays-* ops use sf-q6..q11 suffixes and target the arrays_* tables instead. We separate
    // the two so a run of arrays-only ops does not fail the events_variant emptiness check.
    boolean needsJson =
        operations.stream()
            .anyMatch(
                op ->
                    op.equals("read-baseline")
                        || (op.endsWith("-json") && !op.equals("write-arrays-json")));
    boolean needsVariant =
        operations.stream().anyMatch(op -> op.endsWith("-variant") && !isArraysOp(op));
    boolean needsShredded =
        operations.stream()
            .anyMatch(op -> op.contains("-shredded") && !op.contains("-buf") && !isArraysOp(op));
    boolean hasArraysStaging =
        !spark.catalog().listTables().filter("name = 'staged_arrays'").isEmpty();
    boolean needsArraysVariant =
        hasArraysStaging
            && operations.stream().anyMatch(op -> op.endsWith("-variant") && isArraysOp(op));
    boolean needsArraysShredded =
        hasArraysStaging
            && operations.stream().anyMatch(op -> op.endsWith("-shredded") && isArraysOp(op));
    if (!hasArraysStaging && operations.stream().anyMatch(SparkBenchDriver::isArraysOp)) {
      System.out.println(
          "Skipping arrays read ops (sf-q6..q11): staged_arrays temp view not registered.");
    }

    long jsonCount = -1;
    if (needsJson) {
      jsonCount =
          (long)
              spark.sql("SELECT COUNT(*) FROM bench.db.events_json").collectAsList().get(0).get(0);
      if (jsonCount == 0) {
        throw new RuntimeException("Table bench.db.events_json is empty. Run write-json first.");
      }
    }

    long variantCount = -1;
    if (needsVariant) {
      variantCount =
          (long)
              spark
                  .sql("SELECT COUNT(*) FROM bench.db.events_variant")
                  .collectAsList()
                  .get(0)
                  .get(0);
      if (variantCount == 0) {
        throw new RuntimeException(
            "Table bench.db.events_variant is empty. Run write-variant first.");
      }
    }

    long shreddedCount = -1;
    if (needsShredded) {
      shreddedCount =
          (long)
              spark
                  .sql("SELECT COUNT(*) FROM bench.db.events_shredded")
                  .collectAsList()
                  .get(0)
                  .get(0);
      if (shreddedCount == 0) {
        throw new RuntimeException(
            "Table bench.db.events_shredded is empty. Run write-shredded first.");
      }
    }

    long arraysVariantCount = -1;
    if (needsArraysVariant) {
      arraysVariantCount =
          (long)
              spark
                  .sql("SELECT COUNT(*) FROM bench.db.arrays_variant")
                  .collectAsList()
                  .get(0)
                  .get(0);
      if (arraysVariantCount == 0) {
        throw new RuntimeException(
            "Table bench.db.arrays_variant is empty. Run write-arrays-variant first.");
      }
    }

    long arraysShreddedCount = -1;
    if (needsArraysShredded) {
      arraysShreddedCount =
          (long)
              spark
                  .sql("SELECT COUNT(*) FROM bench.db.arrays_shredded")
                  .collectAsList()
                  .get(0)
                  .get(0);
      if (arraysShreddedCount == 0) {
        throw new RuntimeException(
            "Table bench.db.arrays_shredded is empty. Run write-arrays-shredded first.");
      }
    }

    for (int bufferSize : new int[] {10, 1000, 10000}) {
      String tableName = "bench.db.events_shredded_buf" + bufferSize;
      String suffix = "-shredded-buf" + bufferSize;
      boolean needsBuf = operations.stream().anyMatch(op -> op.endsWith(suffix));
      if (needsBuf) {
        long count =
            (long) spark.sql("SELECT COUNT(*) FROM " + tableName).collectAsList().get(0).get(0);
        if (count == 0) {
          throw new RuntimeException(
              "Table " + tableName + " is empty. Run write-shredded-buf" + bufferSize + " first.");
        }
      }
    }

    StringBuilder rowSummary = new StringBuilder("Reading from populated tables:");
    if (needsJson) {
      rowSummary.append(" events_json=").append(jsonCount).append(" rows");
    }
    if (needsVariant) {
      rowSummary
          .append(needsJson ? "," : "")
          .append(" events_variant=")
          .append(variantCount)
          .append(" rows");
    }
    if (needsShredded) {
      rowSummary
          .append((needsJson || needsVariant) ? "," : "")
          .append(" events_shredded=")
          .append(shreddedCount)
          .append(" rows");
    }
    if (needsArraysVariant) {
      rowSummary
          .append((needsJson || needsVariant || needsShredded) ? "," : "")
          .append(" arrays_variant=")
          .append(arraysVariantCount)
          .append(" rows");
    }
    if (needsArraysShredded) {
      rowSummary
          .append((needsJson || needsVariant || needsShredded || needsArraysVariant) ? "," : "")
          .append(" arrays_shredded=")
          .append(arraysShreddedCount)
          .append(" rows");
    }
    System.out.println(rowSummary);
    System.out.println();

    // Baseline: typed column read (performance ceiling)
    if (operations.contains("read-baseline")) {
      runOperation(
          "read-baseline",
          () -> spark.sql("SELECT SUM(event_id) FROM bench.db.events_json").collect());
    }

    // -- Projection: flat field --
    if (operations.contains("read-project-json")) {
      runOperation(
          "read-project-json",
          () ->
              spark
                  .sql(
                      "SELECT SUM(CAST(get_json_object(payload, '$.duration') AS INT)) "
                          + "FROM bench.db.events_json")
                  .collect());
    }

    if (operations.contains("read-project-variant")) {
      runOperation(
          "read-project-variant",
          () ->
              spark
                  .sql(
                      "SELECT SUM(try_variant_get(payload, '$.duration', 'int')) "
                          + "FROM bench.db.events_variant")
                  .collect());
    }

    if (operations.contains("read-project-shredded")) {
      runOperation(
          "read-project-shredded",
          () ->
              spark
                  .sql(
                      "SELECT SUM(try_variant_get(payload, '$.duration', 'int')) "
                          + "FROM bench.db.events_shredded")
                  .collect());
    }

    // Sensitivity sweep reads: same projection query against the buffer-N tables
    for (int bufferSize : new int[] {10, 1000, 10000}) {
      String opName = "read-project-shredded-buf" + bufferSize;
      if (operations.contains(opName)) {
        String tableName = "bench.db.events_shredded_buf" + bufferSize;
        runOperation(
            opName,
            () ->
                spark
                    .sql(
                        "SELECT SUM(try_variant_get(payload, '$.duration', 'int')) FROM "
                            + tableName)
                    .collect());
      }
    }

    // -- Projection: nested field (2 levels deep) --
    if (operations.contains("read-nested-json")) {
      runOperation(
          "read-nested-json",
          () ->
              spark
                  .sql(
                      "SELECT SUM(CAST(get_json_object(payload, '$.metrics.timing.load_ms') AS INT)) "
                          + "FROM bench.db.events_json")
                  .collect());
    }

    if (operations.contains("read-nested-variant")) {
      runOperation(
          "read-nested-variant",
          () ->
              spark
                  .sql(
                      "SELECT SUM(try_variant_get(payload, '$.metrics.timing.load_ms', 'int')) "
                          + "FROM bench.db.events_variant")
                  .collect());
    }

    if (operations.contains("read-nested-shredded")) {
      runOperation(
          "read-nested-shredded",
          () ->
              spark
                  .sql(
                      "SELECT SUM(try_variant_get(payload, '$.metrics.timing.load_ms', 'int')) "
                          + "FROM bench.db.events_shredded")
                  .collect());
    }

    // -- Filter --
    if (operations.contains("read-filter-json")) {
      runOperation(
          "read-filter-json",
          () ->
              spark
                  .sql(
                      "SELECT COUNT(event_id), "
                          + "SUM(CAST(get_json_object(payload, '$.duration') AS INT)) "
                          + "FROM bench.db.events_json "
                          + "WHERE get_json_object(payload, '$.event_type') = 'purchase' "
                          + "AND get_json_object(payload, '$.country') = 'US'")
                  .collect());
    }

    if (operations.contains("read-filter-variant")) {
      runOperation(
          "read-filter-variant",
          () ->
              spark
                  .sql(
                      "SELECT COUNT(event_id), "
                          + "SUM(try_variant_get(payload, '$.duration', 'int')) "
                          + "FROM bench.db.events_variant "
                          + "WHERE try_variant_get(payload, '$.event_type', 'string') = 'purchase' "
                          + "AND try_variant_get(payload, '$.country', 'string') = 'US'")
                  .collect());
    }

    if (operations.contains("read-filter-shredded")) {
      runOperation(
          "read-filter-shredded",
          () ->
              spark
                  .sql(
                      "SELECT COUNT(event_id), "
                          + "SUM(try_variant_get(payload, '$.duration', 'int')) "
                          + "FROM bench.db.events_shredded "
                          + "WHERE try_variant_get(payload, '$.event_type', 'string') = 'purchase' "
                          + "AND try_variant_get(payload, '$.country', 'string') = 'US'")
                  .collect());
    }

    // -- Aggregation --
    if (operations.contains("read-agg-json")) {
      runOperation(
          "read-agg-json",
          () ->
              spark
                  .sql(
                      "SELECT get_json_object(payload, '$.event_type'), "
                          + "COUNT(event_id), "
                          + "SUM(CAST(get_json_object(payload, '$.duration') AS INT)) "
                          + "FROM bench.db.events_json "
                          + "GROUP BY get_json_object(payload, '$.event_type')")
                  .collect());
    }

    if (operations.contains("read-agg-variant")) {
      runOperation(
          "read-agg-variant",
          () ->
              spark
                  .sql(
                      "SELECT try_variant_get(payload, '$.event_type', 'string'), "
                          + "COUNT(event_id), "
                          + "SUM(try_variant_get(payload, '$.duration', 'int')) "
                          + "FROM bench.db.events_variant "
                          + "GROUP BY try_variant_get(payload, '$.event_type', 'string')")
                  .collect());
    }

    if (operations.contains("read-agg-shredded")) {
      runOperation(
          "read-agg-shredded",
          () ->
              spark
                  .sql(
                      "SELECT try_variant_get(payload, '$.event_type', 'string'), "
                          + "COUNT(event_id), "
                          + "SUM(try_variant_get(payload, '$.duration', 'int')) "
                          + "FROM bench.db.events_shredded "
                          + "GROUP BY try_variant_get(payload, '$.event_type', 'string')")
                  .collect());
    }

    // -- Snowflake Iceberg V3 Variant benchmark Q1-Q5 (events_variant). Q6-Q11 require
    //    a separate arrays_variant table not modeled here. Source:
    //    https://www.snowflake.com/en/blog/engineering/snowflake-iceberg-v3-variant-performance/
    //    Same {variant, shredded} tables as the rest of the suite so results are
    //    directly comparable to Snowflake's published numbers.

    if (operations.contains("sf-q1-variant")) {
      runOperation(
          "sf-q1-variant",
          () ->
              spark
                  .sql(
                      "SELECT SUM(try_variant_get(payload, '$.duration', 'int')) "
                          + "FROM bench.db.events_variant")
                  .collect());
    }
    if (operations.contains("sf-q1-shredded")) {
      runOperation(
          "sf-q1-shredded",
          () ->
              spark
                  .sql(
                      "SELECT SUM(try_variant_get(payload, '$.duration', 'int')) "
                          + "FROM bench.db.events_shredded")
                  .collect());
    }

    if (operations.contains("sf-q2-variant")) {
      runOperation(
          "sf-q2-variant",
          () ->
              spark
                  .sql(
                      "SELECT SUM(try_variant_get(payload, '$.metrics.timing.load_ms', 'int')) "
                          + "FROM bench.db.events_variant")
                  .collect());
    }
    if (operations.contains("sf-q2-shredded")) {
      runOperation(
          "sf-q2-shredded",
          () ->
              spark
                  .sql(
                      "SELECT SUM(try_variant_get(payload, '$.metrics.timing.load_ms', 'int')) "
                          + "FROM bench.db.events_shredded")
                  .collect());
    }

    if (operations.contains("sf-q3-variant")) {
      runOperation(
          "sf-q3-variant",
          () ->
              spark
                  .sql(
                      "SELECT COUNT(event_id), "
                          + "SUM(try_variant_get(payload, '$.duration', 'int')) "
                          + "FROM bench.db.events_variant "
                          + "WHERE try_variant_get(payload, '$.event_type', 'string') = 'purchase' "
                          + "AND try_variant_get(payload, '$.country', 'string') = 'US'")
                  .collect());
    }
    if (operations.contains("sf-q3-shredded")) {
      runOperation(
          "sf-q3-shredded",
          () ->
              spark
                  .sql(
                      "SELECT COUNT(event_id), "
                          + "SUM(try_variant_get(payload, '$.duration', 'int')) "
                          + "FROM bench.db.events_shredded "
                          + "WHERE try_variant_get(payload, '$.event_type', 'string') = 'purchase' "
                          + "AND try_variant_get(payload, '$.country', 'string') = 'US'")
                  .collect());
    }

    if (operations.contains("sf-q4-variant")) {
      runOperation(
          "sf-q4-variant",
          () ->
              spark
                  .sql(
                      "SELECT try_variant_get(payload, '$.event_type', 'string') AS event_type, "
                          + "COUNT(event_id) AS cnt, "
                          + "SUM(try_variant_get(payload, '$.duration', 'int')) AS total_duration "
                          + "FROM bench.db.events_variant "
                          + "GROUP BY try_variant_get(payload, '$.event_type', 'string')")
                  .collect());
    }
    if (operations.contains("sf-q4-shredded")) {
      runOperation(
          "sf-q4-shredded",
          () ->
              spark
                  .sql(
                      "SELECT try_variant_get(payload, '$.event_type', 'string') AS event_type, "
                          + "COUNT(event_id) AS cnt, "
                          + "SUM(try_variant_get(payload, '$.duration', 'int')) AS total_duration "
                          + "FROM bench.db.events_shredded "
                          + "GROUP BY try_variant_get(payload, '$.event_type', 'string')")
                  .collect());
    }

    if (operations.contains("sf-q5-variant")) {
      runOperation(
          "sf-q5-variant",
          () ->
              spark
                  .sql(
                      "SELECT payload FROM bench.db.events_variant "
                          + "ORDER BY try_variant_get(payload, '$.duration', 'int') LIMIT 10")
                  .collect());
    }
    if (operations.contains("sf-q5-shredded")) {
      runOperation(
          "sf-q5-shredded",
          () ->
              spark
                  .sql(
                      "SELECT payload FROM bench.db.events_shredded "
                          + "ORDER BY try_variant_get(payload, '$.duration', 'int') LIMIT 10")
                  .collect());
    }

    // -- Snowflake Q6-Q11: arrays_variant / arrays_shredded family. Direct counterparts to the
    //    published Snowflake numbers. Spark uses try_variant_get with $[i] / $[i][j] path
    //    expressions to access array elements; the shredded family lets the reader skip
    //    decoding non-projected elements when shredding inferred a typed array.
    //
    // Skip Q6-Q11 entirely if the arrays_* tables aren't populated (set to false earlier in
    // this method when staged_arrays wasn't registered).
    boolean canRunArraysVariantReads = needsArraysVariant && arraysVariantCount > 0;
    boolean canRunArraysShreddedReads = needsArraysShredded && arraysShreddedCount > 0;

    if (operations.contains("sf-q6-variant") && canRunArraysVariantReads) {
      runOperation(
          "sf-q6-variant",
          () ->
              spark
                  .sql(
                      "SELECT SUM(try_variant_get(arr_number, '$[0]', 'int')) "
                          + "FROM bench.db.arrays_variant")
                  .collect());
    }
    if (operations.contains("sf-q6-shredded") && canRunArraysShreddedReads) {
      runOperation(
          "sf-q6-shredded",
          () ->
              spark
                  .sql(
                      "SELECT SUM(try_variant_get(arr_number, '$[0]', 'int')) "
                          + "FROM bench.db.arrays_shredded")
                  .collect());
    }

    if (operations.contains("sf-q7-variant") && canRunArraysVariantReads) {
      runOperation(
          "sf-q7-variant",
          () ->
              spark
                  .sql(
                      "SELECT MIN(try_variant_get(arr_text, '$[0]', 'string')) "
                          + "FROM bench.db.arrays_variant")
                  .collect());
    }
    if (operations.contains("sf-q7-shredded") && canRunArraysShreddedReads) {
      runOperation(
          "sf-q7-shredded",
          () ->
              spark
                  .sql(
                      "SELECT MIN(try_variant_get(arr_text, '$[0]', 'string')) "
                          + "FROM bench.db.arrays_shredded")
                  .collect());
    }

    if (operations.contains("sf-q8-variant") && canRunArraysVariantReads) {
      runOperation(
          "sf-q8-variant",
          () ->
              spark
                  .sql(
                      "SELECT SUM(try_variant_get(arr_graph, '$[0][3]', 'int')) "
                          + "FROM bench.db.arrays_variant")
                  .collect());
    }
    if (operations.contains("sf-q8-shredded") && canRunArraysShreddedReads) {
      runOperation(
          "sf-q8-shredded",
          () ->
              spark
                  .sql(
                      "SELECT SUM(try_variant_get(arr_graph, '$[0][3]', 'int')) "
                          + "FROM bench.db.arrays_shredded")
                  .collect());
    }

    if (operations.contains("sf-q9-variant") && canRunArraysVariantReads) {
      runOperation(
          "sf-q9-variant",
          () ->
              spark
                  .sql(
                      "SELECT arr_number FROM bench.db.arrays_variant "
                          + "ORDER BY try_variant_get(arr_number, '$[0]', 'int') LIMIT 10")
                  .collect());
    }
    if (operations.contains("sf-q9-shredded") && canRunArraysShreddedReads) {
      runOperation(
          "sf-q9-shredded",
          () ->
              spark
                  .sql(
                      "SELECT arr_number FROM bench.db.arrays_shredded "
                          + "ORDER BY try_variant_get(arr_number, '$[0]', 'int') LIMIT 10")
                  .collect());
    }

    if (operations.contains("sf-q10-variant") && canRunArraysVariantReads) {
      runOperation(
          "sf-q10-variant",
          () ->
              spark
                  .sql(
                      "SELECT arr_text FROM bench.db.arrays_variant "
                          + "ORDER BY try_variant_get(arr_text, '$[0]', 'string') LIMIT 10")
                  .collect());
    }
    if (operations.contains("sf-q10-shredded") && canRunArraysShreddedReads) {
      runOperation(
          "sf-q10-shredded",
          () ->
              spark
                  .sql(
                      "SELECT arr_text FROM bench.db.arrays_shredded "
                          + "ORDER BY try_variant_get(arr_text, '$[0]', 'string') LIMIT 10")
                  .collect());
    }

    if (operations.contains("sf-q11-variant") && canRunArraysVariantReads) {
      runOperation(
          "sf-q11-variant",
          () ->
              spark
                  .sql(
                      "SELECT arr_graph FROM bench.db.arrays_variant "
                          + "ORDER BY try_variant_get(arr_graph, '$[0][3]', 'int') LIMIT 10")
                  .collect());
    }
    if (operations.contains("sf-q11-shredded") && canRunArraysShreddedReads) {
      runOperation(
          "sf-q11-shredded",
          () ->
              spark
                  .sql(
                      "SELECT arr_graph FROM bench.db.arrays_shredded "
                          + "ORDER BY try_variant_get(arr_graph, '$[0][3]', 'int') LIMIT 10")
                  .collect());
    }

    captureCorrectness();
  }

  public void run() {
    System.out.println("=== Iceberg Baseline Benchmark - Spark Driver ===");
    System.out.printf(
        Locale.ROOT, "  Threads: %d, Warmup: %d, Iterations: %d%n", threads, warmup, iterations);
    System.out.printf(
        Locale.ROOT,
        "  Compression: %s, Format version: 3, Vectorization: disabled%n",
        compression);
    System.out.printf(Locale.ROOT, "  Warehouse: %s%n", warehouse);
    System.out.printf(Locale.ROOT, "  Staging: %s%n", staging);
    System.out.printf(Locale.ROOT, "  Operations: %s%n", operations);
    System.out.println();

    initSpark();
    try {
      createTables();
      runWriteBenchmarks();
      runReadBenchmarks();
    } finally {
      spark.stop();
    }

    System.out.println("=== Benchmark complete. Results in: " + resultsBase + " ===");
  }

  public static void main(String[] args) {
    new SparkBenchDriver().run();
  }
}
