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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Generates benchmark staging data: Parquet files with 2 columns (event_id BIGINT, payload STRING).
 *
 * <p>The payload is a nested JSON string representing a realistic semi-structured event. Both the
 * JSON-string table and the variant table are populated from this same source - one stores the
 * payload as-is (STRING), the other runs parse_json() on it (VARIANT). This ensures data parity.
 */
public class DataGenerator {

  private static final String[] EVENT_TYPES = {
    "page_view", "click", "scroll", "purchase", "add_to_cart",
    "remove_from_cart", "search", "login", "logout", "signup",
    "share", "comment", "like", "bookmark", "download",
    "upload", "print", "export", "import", "settings_change"
  };

  private static final String[] COUNTRIES = {
    "US", "CN", "IN", "BR", "JP", "RU", "DE", "GB", "FR", "MX",
    "ID", "NG", "PK", "BD", "VN", "KR", "TR", "IT", "ES", "TH",
    "PH", "EG", "AR", "PL", "CA", "MY", "AU", "NL", "SA", "PE",
    "CO", "ZA", "UA", "RO", "CL", "CZ", "BE", "SE", "PT", "AT",
    "CH", "IL", "HK", "SG", "NZ", "IE", "DK", "FI", "NO", "GR"
  };

  private static final StructType SCHEMA =
      new StructType(
          new StructField[] {
            new StructField("event_id", DataTypes.LongType, false, Metadata.empty()),
            new StructField("payload", DataTypes.StringType, false, Metadata.empty()),
          });

  /**
   * Schema for the Snowflake arrays_variant benchmark table (Q6-Q11). Three array-shaped columns:
   *
   * <ul>
   *   <li>{@code arr_text}: 256-element array of 16-char hex strings (Snowflake Q7/Q10).
   *   <li>{@code arr_number}: 256-element array of ints in [0, 999999] (Snowflake Q6/Q9).
   *   <li>{@code arr_graph}: 64x64 2D array of ints in [0, 999999] (Snowflake Q8/Q11).
   * </ul>
   *
   * <p>Schema choice mirrors the published Snowflake Iceberg V3 Variant benchmark
   * (snowflake.com/en/blog/engineering/snowflake-iceberg-v3-variant-performance) so our timings are
   * directly comparable. The Spark side stores arrays natively; downstream write ops cast to
   * VARIANT via parse_json / to_variant.
   */
  private static final StructType SCHEMA_ARRAYS =
      new StructType(
          new StructField[] {
            new StructField("id", DataTypes.LongType, false, Metadata.empty()),
            new StructField(
                "arr_text",
                DataTypes.createArrayType(DataTypes.StringType),
                false,
                Metadata.empty()),
            new StructField(
                "arr_number",
                DataTypes.createArrayType(DataTypes.IntegerType),
                false,
                Metadata.empty()),
            new StructField(
                "arr_graph",
                DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.IntegerType)),
                false,
                Metadata.empty()),
          });

  private static final int ARR_TEXT_LENGTH = 256;
  private static final int ARR_NUMBER_LENGTH = 256;
  private static final int ARR_GRAPH_ROWS = 64;
  private static final int ARR_GRAPH_COLS = 64;
  private static final int ARR_TEXT_HEX_LENGTH = 16;
  private static final int ARR_VALUE_MAX_EXCLUSIVE = 1_000_000;
  private static final String HEX_CHARS = "0123456789abcdef";

  /**
   * Build a nested JSON payload string. Contains flat scalars, 2-level nested objects, arrays,
   * booleans, and mixed types to exercise variant serialization realistically.
   */
  private static String buildPayload(Random rng, long eventId) {
    // 80/20 skew on event types
    int etIdx = rng.nextDouble() < 0.8 ? rng.nextInt(4) : rng.nextInt(EVENT_TYPES.length);
    String eventType = EVENT_TYPES[etIdx];
    String country = COUNTRIES[rng.nextInt(COUNTRIES.length)];
    int duration = rng.nextInt(300_000);
    long userId = Math.floorMod(rng.nextLong(), 10_000_000L);
    boolean isPremium = rng.nextDouble() < 0.15;
    String session = Long.toHexString(rng.nextLong());
    int loadMs = rng.nextInt(5000);
    int renderMs = rng.nextInt(10000);
    boolean isBot = rng.nextDouble() < 0.05;
    int tag1 = rng.nextInt(50);
    int tag2 = rng.nextInt(50);

    return String.format(
        Locale.ROOT,
        "{\"event_type\":\"%s\",\"country\":\"%s\",\"duration\":%d,"
            + "\"user\":{\"id\":%d,\"is_premium\":%s,\"session\":\"%s\"},"
            + "\"metrics\":{\"timing\":{\"load_ms\":%d,\"render_ms\":%d},"
            + "\"flags\":{\"is_bot\":%s}},"
            + "\"tags\":[\"tag_%d\",\"tag_%d\"],"
            + "\"label\":\"item_%d\"}",
        eventType,
        country,
        duration,
        userId,
        isPremium,
        session,
        loadMs,
        renderMs,
        isBot,
        tag1,
        tag2,
        eventId % 10000);
  }

  /**
   * Generate staging data as Parquet files.
   *
   * @param spark SparkSession
   * @param stagingDir output directory (e.g., /tmp/iceberg-bench/staging-v2-small)
   * @param numRows total rows
   * @param seed random seed for reproducibility
   */
  public static void generateEvents(
      SparkSession spark, String stagingDir, long numRows, long seed) {
    Random rng = new Random(seed);
    int batchSize = 500_000;
    int batches = (int) ((numRows + batchSize - 1) / batchSize);

    for (int batch = 0; batch < batches; batch++) {
      long startId = (long) batch * batchSize;
      long endId = Math.min(startId + batchSize, numRows);
      List<Row> rows = new ArrayList<>((int) (endId - startId));

      for (long id = startId; id < endId; id++) {
        rows.add(RowFactory.create(id, buildPayload(rng, id)));
      }

      Dataset<Row> df = spark.createDataFrame(rows, SCHEMA);
      df.coalesce(Math.max(1, (int) ((endId - startId) / 250_000)))
          .write()
          .mode("append")
          .parquet(stagingDir + "/events");
    }

    System.out.printf(Locale.ROOT, "Generated %d rows to %s/events%n", numRows, stagingDir);
  }

  /**
   * Generate staging data for the Snowflake arrays_variant benchmark table. Output schema before
   * downstream VARIANT casting:
   *
   * <pre>{@code
   * (id BIGINT, arr_text array<string>, arr_number array<int>, arr_graph array<array<int>>)
   * }</pre>
   *
   * <p>Each row contains a 256-element string array (16-char hex strings), a 256-element int array
   * (values in [0, 999999]), and a 64x64 2D int matrix. Mirrors the published Snowflake Iceberg V3
   * Variant benchmark (Q6-Q11) so timings are directly comparable. Files are written to {@code
   * <stagingDir>/events/} (matching the events_variant convention so the matrix runner finds them).
   * Uses a deterministic seeded {@link Random} so re-runs produce identical files.
   *
   * @param spark SparkSession
   * @param stagingDir output directory (e.g., /tmp/iceberg-bench/staging-v2-small)
   * @param numRows total rows (10M for parity with the published Snowflake benchmark)
   * @param seed random seed for reproducibility
   */
  public static void generateArraysVariant(
      SparkSession spark, String stagingDir, long numRows, long seed) {
    Random rng = new Random(seed);
    int batchSize = 50_000;
    int batches = (int) ((numRows + batchSize - 1) / batchSize);

    for (int batch = 0; batch < batches; batch++) {
      long startId = (long) batch * batchSize;
      long endId = Math.min(startId + batchSize, numRows);
      List<Row> rows = new ArrayList<>((int) (endId - startId));

      for (long id = startId; id < endId; id++) {
        rows.add(RowFactory.create(id, buildArrText(rng), buildArrNumber(rng), buildArrGraph(rng)));
      }

      Dataset<Row> df = spark.createDataFrame(rows, SCHEMA_ARRAYS);
      df.coalesce(Math.max(1, (int) ((endId - startId) / 25_000)))
          .write()
          .mode("append")
          .parquet(stagingDir + "/events");
    }

    System.out.printf(Locale.ROOT, "Generated %d rows to %s/events%n", numRows, stagingDir);
  }

  private static List<String> buildArrText(Random rng) {
    List<String> out = new ArrayList<>(ARR_TEXT_LENGTH);
    for (int i = 0; i < ARR_TEXT_LENGTH; i++) {
      out.add(randomHex(rng, ARR_TEXT_HEX_LENGTH));
    }
    return out;
  }

  private static List<Integer> buildArrNumber(Random rng) {
    List<Integer> out = new ArrayList<>(ARR_NUMBER_LENGTH);
    for (int i = 0; i < ARR_NUMBER_LENGTH; i++) {
      out.add(rng.nextInt(ARR_VALUE_MAX_EXCLUSIVE));
    }
    return out;
  }

  private static List<List<Integer>> buildArrGraph(Random rng) {
    List<List<Integer>> out = new ArrayList<>(ARR_GRAPH_ROWS);
    for (int i = 0; i < ARR_GRAPH_ROWS; i++) {
      List<Integer> row = new ArrayList<>(ARR_GRAPH_COLS);
      for (int j = 0; j < ARR_GRAPH_COLS; j++) {
        row.add(rng.nextInt(ARR_VALUE_MAX_EXCLUSIVE));
      }
      out.add(row);
    }
    return out;
  }

  private static String randomHex(Random rng, int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(HEX_CHARS.charAt(rng.nextInt(HEX_CHARS.length())));
    }
    return sb.toString();
  }

  public static void main(String[] args) {
    String scale = System.getProperty("bench.scale", "small");
    String stagingDir = System.getProperty("bench.staging", "/tmp/iceberg-bench/staging-v2-small");
    long seed = Long.parseLong(System.getProperty("bench.seed", "12345"));
    String dataset = System.getProperty("bench.dataset", "events");

    long rows;
    switch (scale) {
      case "tiny":
        rows = 1_000L;
        break;
      case "nano":
        rows = 100_000L;
        break;
      case "micro":
        rows = 1_000_000L;
        break;
      case "small":
        rows = 10_000_000L;
        break;
      case "medium":
        rows = 100_000_000L;
        break;
      case "large":
        rows = 500_000_000L;
        break;
      default:
        throw new IllegalArgumentException(
            "Unknown scale: " + scale + " (valid: tiny, nano, micro, small, medium, large)");
    }

    SparkSession spark =
        SparkSession.builder()
            .appName("IcebergBenchmarkDataGenerator")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .getOrCreate();

    try {
      System.out.printf(
          Locale.ROOT,
          "Generating benchmark data: dataset=%s, scale=%s, rows=%d, staging=%s%n",
          dataset,
          scale,
          rows,
          stagingDir);
      switch (dataset) {
        case "events":
          generateEvents(spark, stagingDir, rows, seed);
          break;
        case "arrays":
          generateArraysVariant(spark, stagingDir, rows, seed);
          break;
        default:
          throw new IllegalArgumentException(
              "Unknown dataset: " + dataset + " (valid: events, arrays)");
      }
      System.out.println("Data generation complete.");
    } finally {
      spark.stop();
    }
  }
}
