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
        eventType, country, duration,
        userId, isPremium, session,
        loadMs, renderMs, isBot,
        tag1, tag2, eventId % 10000);
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

    System.out.printf(
        Locale.ROOT, "Generated %d rows to %s/events%n", numRows, stagingDir);
  }

  public static void main(String[] args) {
    String scale = System.getProperty("bench.scale", "small");
    String stagingDir = System.getProperty("bench.staging", "/tmp/iceberg-bench/staging-v2-small");
    long seed = Long.parseLong(System.getProperty("bench.seed", "12345"));

    long rows;
    switch (scale) {
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
        throw new IllegalArgumentException("Unknown scale: " + scale);
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
          "Generating benchmark data: scale=%s, rows=%d, staging=%s%n",
          scale, rows, stagingDir);
      generateEvents(spark, stagingDir, rows, seed);
      System.out.println("Data generation complete.");
    } finally {
      spark.stop();
    }
  }
}
