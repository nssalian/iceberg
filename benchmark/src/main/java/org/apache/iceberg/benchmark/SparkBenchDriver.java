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
  private final List<String> operations;

  private SparkSession spark;

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

    String ops = System.getProperty("bench.operations", "all");
    if ("all".equals(ops)) {
      this.operations =
          Arrays.asList(
              "write-json", "write-variant",
              "read-baseline",
              "read-project-json", "read-project-variant",
              "read-nested-json", "read-nested-variant",
              "read-filter-json", "read-filter-variant",
              "read-agg-json", "read-agg-variant");
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
            COMMON_PROPS, compression));

    // Variant table
    spark.sql(
        String.format(
            Locale.ROOT,
            "CREATE TABLE IF NOT EXISTS bench.db.events_variant ("
                + "event_id BIGINT, payload VARIANT"
                + ") USING iceberg TBLPROPERTIES (%s, "
                + "'write.parquet.compression-codec' = '%s')",
            COMMON_PROPS, compression));

    // Register staging data as temp view
    spark.read().parquet(staging + "/events").createOrReplaceTempView("staged_events");
  }

  private List<Long> runOperation(String name, Runnable operation) {
    System.out.println("--- Running: " + name + " ---");

    for (int i = 0; i < warmup; i++) {
      System.out.printf(Locale.ROOT, "  Warmup %d/%d%n", i + 1, warmup);
      operation.run();
    }

    List<Long> timings = new ArrayList<>();

    for (int i = 0; i < iterations; i++) {
      long start = System.nanoTime();
      operation.run();
      long elapsed = System.nanoTime() - start;
      timings.add(elapsed);
      System.out.printf(
          Locale.ROOT, "  Iteration %d/%d: %d ms%n", i + 1, iterations, elapsed / 1_000_000);
    }

    Path outputDir = Paths.get(resultsBase, name, "parquet-" + compression);
    try {
      BenchmarkResultWriter.writeTimings(outputDir, name, timings);
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
                    COMMON_PROPS, compression));
            spark.sql(
                "INSERT INTO bench.db.events_json SELECT event_id, payload FROM staged_events");
          });
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
                    COMMON_PROPS, compression));
            spark.sql(
                "INSERT INTO bench.db.events_variant "
                    + "SELECT event_id, parse_json(payload) FROM staged_events");
          });
    }
  }

  // ---------------------------------------------------------------------------
  // Reads - query tables populated by writes.
  // Fail if tables are empty (writes must run first).
  // ---------------------------------------------------------------------------

  private void runReadBenchmarks() {
    boolean hasAnyRead =
        operations.stream().anyMatch(op -> op.startsWith("read-"));
    if (!hasAnyRead) {
      return;
    }

    // Verify tables have data
    long jsonCount =
        (long) spark.sql("SELECT COUNT(*) FROM bench.db.events_json").collectAsList().get(0).get(0);
    long variantCount =
        (long)
            spark
                .sql("SELECT COUNT(*) FROM bench.db.events_variant")
                .collectAsList()
                .get(0)
                .get(0);

    if (jsonCount == 0 || variantCount == 0) {
      throw new RuntimeException(
          String.format(
              Locale.ROOT,
              "Tables are empty (events_json=%d rows, events_variant=%d rows). "
                  + "Run write benchmarks first: --ops write-json,write-variant",
              jsonCount, variantCount));
    }

    System.out.printf(
        Locale.ROOT,
        "Reading from populated tables: events_json=%d rows, events_variant=%d rows%n%n",
        jsonCount, variantCount);

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
  }

  public void run() {
    System.out.println("=== Iceberg Baseline Benchmark - Spark Driver ===");
    System.out.printf(
        Locale.ROOT,
        "  Threads: %d, Warmup: %d, Iterations: %d%n",
        threads, warmup, iterations);
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
