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
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public final class VariantExtractionQuickCheck {

  private static final int WARMUP_RUNS = 1;
  private static final int MEASURED_RUNS = 5;

  // consumed sink to prevent JIT from eliding the decode loop
  private static long BLACKHOLE;

  private VariantExtractionQuickCheck() {}

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println(
          "usage: VariantExtractionQuickCheck <parquet-file> <variant-column> <shredded-field>"
              + " [outputDir] [workloadLabel] [strategyLabel] [projectFields] [filterField] [aggField]");
      System.err.println("  <parquet-file>    absolute path to a shredded parquet file");
      System.err.println("  <variant-column>  top-level variant column name, e.g. payload");
      System.err.println("  <shredded-field>  sub-field to probe, e.g. duration or event_type");
      System.err.println("  [outputDir]       where to write CSVs, defaults to benchmark/results");
      System.err.println(
          "  [projectFields]   comma-separated fields to simulate variant_get() projection,");
      System.err.println(
          "                    e.g. 'duration,event_type,country' - mimics Qiegang branch");
      System.err.println(
          "  [filterField]     top-level primitive field for the filter+project reader-only path");
      System.err.println(
          "                    (defaults to first projectField). Predicate keeps ~50%% of rows.");
      System.err.println(
          "  [aggField]        top-level primitive numeric field for the SUM aggregate reader-only");
      System.err.println("                    path (defaults to first projectField).");
      System.exit(1);
    }

    String filePath = args[0];
    String variantColumn = args[1];
    String probeField = args[2];
    String outputDir = args.length >= 4 ? args[3] : "benchmark/results";
    String workloadLabel = args.length >= 5 ? args[4] : "unknown";
    String strategyLabel = args.length >= 6 ? args[5] : "unknown";
    String projectFieldsCsv = args.length >= 7 ? args[6] : probeField;
    String filterFieldName = args.length >= 8 ? args[7] : projectFieldsCsv.split(",", -1)[0].trim();
    String aggFieldName = args.length >= 9 ? args[8] : projectFieldsCsv.split(",", -1)[0].trim();
    String arraysFixturePathArg = args.length >= 10 ? args[9] : "";

    Configuration conf = new Configuration();
    org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(filePath);

    Fixture fixture = loadFixture(hadoopPath, conf, variantColumn, probeField);
    fixture.workload = workloadLabel;
    fixture.strategy = strategyLabel;
    fixture.projectFields = resolveProjectFields(fixture, projectFieldsCsv);
    fixture.filterField = resolveOneField(fixture, filterFieldName);
    fixture.filterMinValue =
        Long.MIN_VALUE; // recomputed per selectivity in drainFilterProjectNFields
    fixture.aggField = resolveOneField(fixture, aggFieldName);
    fixture.arraysFixturePath = arraysFixturePathArg.isEmpty() ? null : arraysFixturePathArg;

    List<ColumnScore> scores = computePerColumnScore(fixture);

    // Use a dynamic list so we can add paths without touching indices.
    List<TimingResult> results = new ArrayList<>();

    // Reader-only paths A-G all require at least one shredded typed column. Strategies that
    // legitimately choose to shred nothing (e.g. V2_CARDGATED on all-unique UUID workloads)
    // produce a fixture with no typed_value tree; the honest answer for those cells is
    // "reader-only-shredded paths do not apply". Arrays paths (H) and write path (I) still fire.
    boolean hasTypedColumns = !fixture.allShreddedTypedPaths.isEmpty();
    if (!hasTypedColumns) {
      System.err.println(
          "no shredded typed columns present in fixture - strategy '"
              + strategyLabel
              + "' chose to shred nothing on workload '"
              + workloadLabel
              + "'. Skipping reader-only shredded paths (A-G); arrays (H) and write (I) still run.");
    }

    if (hasTypedColumns) {
      // path A: typed-only (single field raw decode)
      results.add(measure("typed_only", () -> readTypedOnly(hadoopPath, conf, fixture)));
      // path B: all shredded leaves, no reconstruction
      results.add(
          measure(
              "all_shredded_no_reconstruction",
              () -> readAllShreddedNoReconstruction(hadoopPath, conf, fixture)));
      // path C: full reconstruction (metadata+value+typed leaves reassembled into Variant)
      results.add(
          measure("with_reconstruction", () -> readWithReconstruction(hadoopPath, conf, fixture)));
      // path D: full Spark boundary simulation - reconstruct then reserialize to fresh byte[]
      results.add(
          measure(
              "with_spark_reserialize", () -> readWithSparkReserialize(hadoopPath, conf, fixture)));
      // path E: project N fields (Qiegang-branch simulation) - direct typed-column read into native
      // values, no VariantVal
      results.add(measure("project_n_fields", () -> readProjectNFields(hadoopPath, conf, fixture)));
      // path F: filter + project N fields - Ryan's exact sync ask (read filter col, apply
      // predicate,
      // project matching rows' fields into Blackhole). Mimics Spark-free variant_get-with-WHERE.
      // Selectivity sweep at 10 / 50 / 90% keeps rows via a target-fraction sample of filter
      // values.
      for (int selPct : new int[] {10, 50, 90}) {
        final int sel = selPct;
        fixture.filterSelectivityPct = sel;
        results.add(
            measure(
                String.format(Locale.ROOT, "filter_project_n_fields_sel%d", sel),
                () -> readFilterProjectNFields(hadoopPath, conf, fixture)));
      }
      // path F2: filter on a STRING field (in addition to the default numeric filter), same
      // selectivity sweep. Only fires if a shredded string field exists AND its type is one
      // computeFilterThreshold can sort properly. String selectivity is a known gap - we skip
      // the sweep rather than emit three identical 100%-pass measurements labeled sel10/50/90.
      ShreddedFieldCol stringFilter = findFirstStringField(fixture);
      if (stringFilter != null) {
        ShreddedFieldCol originalFilter = fixture.filterField;
        fixture.filterField = stringFilter;
        long stringThresholdProbe = computeFilterThreshold(hadoopPath, conf, fixture);
        if (stringThresholdProbe != Long.MIN_VALUE) {
          for (int selPct : new int[] {10, 50, 90}) {
            final int sel = selPct;
            fixture.filterSelectivityPct = sel;
            results.add(
                measure(
                    String.format(Locale.ROOT, "filter_project_n_fields_str_sel%d", sel),
                    () -> readFilterProjectNFields(hadoopPath, conf, fixture)));
          }
        } else {
          System.err.println(
              "[BENCH] filter_project_n_fields_str_sel* skipped for field="
                  + stringFilter.name
                  + " (no typed sort implemented; would emit identical timings under three sel labels)");
        }
        fixture.filterField = originalFilter;
      }
      // path G: SUM aggregate - read one typed column, accumulate into a long. Mimics Spark-free
      // SELECT SUM(variant_get(col, '$.field', 'int')) FROM t.
      results.add(measure("agg_sum_typed", () -> readAggSumTyped(hadoopPath, conf, fixture)));
    }
    // path H: arrays Q6-Q11 reader-only - element access on shredded arrays (Snowflake's
    // arr_number/arr_text/arr_graph corpus). Only fires when arrays_shredded fixture path is set.
    if (fixture.arraysFixturePath != null) {
      results.add(
          measure(
              "arrays_q6_first_element_int", () -> readArraysFirstElement(conf, fixture, false)));
      results.add(
          measure(
              "arrays_q7_first_element_string", () -> readArraysFirstElement(conf, fixture, true)));
    }
    // Path I (write_shredded_no_spark) was removed: it built its shredded schema from the CLI
    // projectFields list and never invoked VariantShreddingAnalyzer, so every strategy produced
    // identical output. The Spark-side write-shredded op remains the authoritative write
    // measurement.

    String timestamp =
        LocalDateTime.now(ZoneOffset.UTC)
            .format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss", Locale.ROOT));
    Path outputPath = Paths.get(outputDir).toAbsolutePath();
    Files.createDirectories(outputPath);
    Path timingsCsv = outputPath.resolve("quickcheck-" + timestamp + "-timings.csv");
    Path scoresCsv = outputPath.resolve("quickcheck-" + timestamp + "-scores.csv");
    writeTimingsCsv(timingsCsv, fixture, results);
    writeScoresCsv(scoresCsv, fixture, scores);

    printSummary(fixture, results, scores);
    System.out.println();
    System.out.println("timings CSV: " + timingsCsv.toAbsolutePath());
    System.out.println("scores CSV: " + scoresCsv.toAbsolutePath());
    System.out.println("blackhole  : " + BLACKHOLE);
  }

  // ---------------------------------------------------------------------------
  // fixture and schema navigation

  private static final class Fixture {
    final String filePath;
    final long fileSizeBytes;
    final long totalRows;
    final int rowGroupCount;
    final MessageType schema;
    final List<BlockMetaData> blocks;

    String variantColumn;
    String probeField;
    ColumnDescriptor probeTypedColumn;
    ColumnDescriptor probeValueColumn;
    ColumnDescriptor metadataColumn;
    ColumnDescriptor topValueColumn;
    List<String[]> allShreddedTypedPaths = new ArrayList<>();
    List<String[]> allShreddedValuePaths = new ArrayList<>();

    // resolved fields for the project_n_fields path (Qiegang-simulation)
    List<ShreddedFieldCol> projectFields = new ArrayList<>();

    // Filter config for the filter_project_n_fields path (Ryan's exact ask: filter+project into
    // Blackhole).
    // filterField is a top-level primitive shredded field; predicate keeps ~filterSelectivityPct%
    // of rows.
    // filterMinValue is populated from filterThresholdCache before each drain fires.
    ShreddedFieldCol filterField;
    int filterSelectivityPct = 50;
    long filterMinValue;
    // Cache thresholds by (fieldName, selectivityPct) so repeated timed runs at the same
    // selectivity
    // don't pay the pre-scan cost. Populated by readFilterProjectNFields via a one-shot column
    // read.
    Map<String, Long> filterThresholdCache = new java.util.HashMap<>();

    // Agg config: sum the typed values of aggField across all rows (SUM aggregate, Ryan's
    // Q4-shape).
    ShreddedFieldCol aggField;

    // Optional arrays_shredded fixture path for the Snowflake Q6-Q11 reader-only paths.
    String arraysFixturePath;

    String workload;
    String strategy;

    Fixture(
        String filePath,
        long fileSizeBytes,
        long totalRows,
        int rowGroupCount,
        MessageType schema,
        List<BlockMetaData> blocks) {
      this.filePath = filePath;
      this.fileSizeBytes = fileSizeBytes;
      this.totalRows = totalRows;
      this.rowGroupCount = rowGroupCount;
      this.schema = schema;
      this.blocks = blocks;
    }
  }

  private static Fixture loadFixture(
      org.apache.hadoop.fs.Path file, Configuration conf, String variantColumn, String probeField)
      throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(file, conf))) {
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();
      List<BlockMetaData> blocks = reader.getRowGroups();
      long totalRows = reader.getRecordCount();
      long onDisk = HadoopInputFile.fromPath(file, conf).getLength();

      Fixture fixture =
          new Fixture(file.toString(), onDisk, totalRows, blocks.size(), schema, blocks);
      fixture.variantColumn = variantColumn;
      fixture.probeField = probeField;

      GroupType variantGroup = (GroupType) schema.getType(variantColumn);
      fixture.metadataColumn =
          schema.getColumnDescription(new String[] {variantColumn, "metadata"});
      fixture.topValueColumn =
          hasChild(variantGroup, "value")
              ? schema.getColumnDescription(new String[] {variantColumn, "value"})
              : null;

      if (!hasChild(variantGroup, "typed_value")) {
        // Strategy chose to shred nothing (e.g. V2_CARDGATED on a high-cardinality unique-value
        // workload). This IS the strategy's answer - not an error. Return the fixture with no
        // typed paths; main() gates reader-only paths on allShreddedTypedPaths.isEmpty().
        // Arrays paths (path H) and write path (path I) still fire because their fixtures are
        // independent of this file's shredded columns.
        return fixture;
      }

      Type typedValueType = variantGroup.getType("typed_value");
      if (!typedValueType.isPrimitive() && typedValueType instanceof GroupType typedValueGroup) {
        // Recursively walk typed_value groups to find every primitive shredded leaf,
        // at any depth (top-level primitives AND nested object shreds).
        List<String> rootPath = new ArrayList<>();
        rootPath.add(variantColumn);
        collectShreddedLeaves(typedValueGroup, rootPath, fixture);

        // Locate probe field at TOP LEVEL only (probe field must be a top-level primitive shred).
        for (Type field : typedValueGroup.getFields()) {
          if (!(field instanceof GroupType fieldGroup)) {
            continue;
          }
          if (!fieldGroup.getName().equals(probeField)) {
            continue;
          }
          boolean typedIsPrimitive =
              hasChild(fieldGroup, "typed_value")
                  && fieldGroup.getType("typed_value").isPrimitive();
          if (!typedIsPrimitive) {
            throw new IllegalArgumentException(
                "Probe field '"
                    + probeField
                    + "' has non-primitive typed_value (nested object) - not supported yet."
                    + " Pick a primitive shredded field.");
          }
          fixture.probeTypedColumn =
              schema.getColumnDescription(
                  new String[] {variantColumn, "typed_value", probeField, "typed_value"});
          if (hasChild(fieldGroup, "value")) {
            fixture.probeValueColumn =
                schema.getColumnDescription(
                    new String[] {variantColumn, "typed_value", probeField, "value"});
          }
        }
      }

      if (fixture.probeTypedColumn == null && !fixture.allShreddedTypedPaths.isEmpty()) {
        // Some columns ARE shredded, but the requested probe field wasn't. That's a caller
        // config problem (probe field name doesn't match any shredded top-level primitive) -
        // fail loudly with the list of what's available.
        StringBuilder available = new StringBuilder();
        for (String[] path : fixture.allShreddedTypedPaths) {
          if (available.length() > 0) {
            available.append(", ");
          }
          available.append(path[path.length - 2]);
        }
        throw new IllegalArgumentException(
            "No primitive shredded typed_value found for '"
                + variantColumn
                + "."
                + probeField
                + "'. Primitive shredded fields present: ["
                + available
                + "]");
      }
      return fixture;
    }
  }

  private static boolean hasChild(GroupType group, String name) {
    for (Type child : group.getFields()) {
      if (child.getName().equals(name)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Convert a Parquet column path array into a human-readable field path. Strips the top-level
   * variant column name and every "typed_value" wrapper, leaving just the semantic field path
   * (e.g., ["payload","typed_value","actor","typed_value","avatar_url","typed_value"] becomes
   * "actor.avatar_url").
   */
  private static String joinFieldPath(String[] typedPath) {
    StringBuilder builder = new StringBuilder();
    for (int index = 1; index < typedPath.length - 1; index++) {
      if (typedPath[index].equals("typed_value")) {
        continue;
      }
      if (builder.length() > 0) {
        builder.append('.');
      }
      builder.append(typedPath[index]);
    }
    return builder.toString();
  }

  /**
   * Recursively walks a typed_value group and collects every primitive shredded leaf (at any
   * depth). Adds paths to {@code fixture.allShreddedTypedPaths} and every sibling {@code value}
   * column to {@code fixture.allShreddedValuePaths}.
   *
   * @param typedGroup a typed_value group (children are field groups each with value + typed_value)
   * @param pathToTypedGroup path from schema root to this typed_value group (excludes
   *     "typed_value")
   */
  private static void collectShreddedLeaves(
      GroupType typedGroup, List<String> pathToTypedGroup, Fixture fixture) {
    for (Type field : typedGroup.getFields()) {
      if (!(field instanceof GroupType fieldGroup)) {
        continue;
      }
      String fieldName = fieldGroup.getName();
      List<String> fieldPath = new ArrayList<>(pathToTypedGroup);
      fieldPath.add("typed_value");
      fieldPath.add(fieldName);

      // Every field group has a value column (fallback bytes)
      if (hasChild(fieldGroup, "value")) {
        List<String> valuePath = new ArrayList<>(fieldPath);
        valuePath.add("value");
        fixture.allShreddedValuePaths.add(valuePath.toArray(new String[0]));
      }

      if (!hasChild(fieldGroup, "typed_value")) {
        continue;
      }
      Type childTyped = fieldGroup.getType("typed_value");
      if (childTyped.isPrimitive()) {
        // primitive leaf
        List<String> typedPath = new ArrayList<>(fieldPath);
        typedPath.add("typed_value");
        fixture.allShreddedTypedPaths.add(typedPath.toArray(new String[0]));
      } else if (childTyped instanceof GroupType childGroup) {
        // nested object - recurse
        collectShreddedLeaves(childGroup, fieldPath, fixture);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Per-column shred score

  private static final class ColumnScore {
    final String fieldName;
    final long valueNulls;
    final long valueTotal;
    final long typedNulls;
    final long typedTotal;
    final boolean typedHasStats;
    final int score;
    final String reason;

    ColumnScore(
        String fieldName,
        long valueNulls,
        long valueTotal,
        long typedNulls,
        long typedTotal,
        boolean typedHasStats,
        int score,
        String reason) {
      this.fieldName = fieldName;
      this.valueNulls = valueNulls;
      this.valueTotal = valueTotal;
      this.typedNulls = typedNulls;
      this.typedTotal = typedTotal;
      this.typedHasStats = typedHasStats;
      this.score = score;
      this.reason = reason;
    }
  }

  private static List<ColumnScore> computePerColumnScore(Fixture fixture) {
    List<ColumnScore> scores = new ArrayList<>();
    for (int i = 0; i < fixture.allShreddedTypedPaths.size(); i++) {
      String[] typedPath = fixture.allShreddedTypedPaths.get(i);
      String[] valuePath =
          i < fixture.allShreddedValuePaths.size() ? fixture.allShreddedValuePaths.get(i) : null;
      // Full dot-path (e.g., "actor.avatar_url") to keep nested-field names unique.
      String fieldName = joinFieldPath(typedPath);

      ColumnPath typedColumnPath = ColumnPath.get(typedPath);
      ColumnPath valueColumnPath = valuePath == null ? null : ColumnPath.get(valuePath);

      long valueNulls = 0;
      long valueTotal = 0;
      long typedNulls = 0;
      long typedTotal = 0;
      boolean typedHasStats = true;

      for (BlockMetaData block : fixture.blocks) {
        for (ColumnChunkMetaData chunk : block.getColumns()) {
          if (chunk.getPath().equals(typedColumnPath)) {
            typedNulls += chunk.getStatistics().getNumNulls();
            typedTotal += chunk.getValueCount();
            Statistics<?> stats = chunk.getStatistics();
            if (stats == null || stats.isEmpty() || !stats.hasNonNullValue()) {
              typedHasStats = false;
            }
          } else if (valueColumnPath != null && chunk.getPath().equals(valueColumnPath)) {
            valueNulls += chunk.getStatistics().getNumNulls();
            valueTotal += chunk.getValueCount();
          }
        }
      }

      boolean noFallbacks = valueColumnPath == null || valueNulls == valueTotal;
      int score;
      String reason;
      if (noFallbacks && typedHasStats) {
        score = 1;
        reason = "shredded, no fallbacks, stats present";
      } else if (!noFallbacks && !typedHasStats) {
        score = -1;
        reason = "fallbacks present and stats missing/invalid";
      } else if (!noFallbacks) {
        score = -1;
        reason = "fallbacks present (value column has " + (valueTotal - valueNulls) + " non-nulls)";
      } else {
        score = -1;
        reason = "stats missing or invalid on typed_value";
      }
      scores.add(
          new ColumnScore(
              fieldName,
              valueNulls,
              valueTotal,
              typedNulls,
              typedTotal,
              typedHasStats,
              score,
              reason));
    }
    return scores;
  }

  // ---------------------------------------------------------------------------
  // read paths

  private interface ReadPath {
    ReadStats read() throws IOException;
  }

  private static final class ReadStats {
    long valuesDecoded;
    long rowsRead;
    long rowGroupsRead;
    long columnChunksRead;
    long compressedBytes;
    long uncompressedBytes;
  }

  private static ReadStats readTypedOnly(
      org.apache.hadoop.fs.Path file, Configuration conf, Fixture fixture) throws IOException {
    return readColumns(
        file, conf, fixture, Collections.singletonList(fixture.probeTypedColumn), ReadMode.DRAIN);
  }

  private static ReadStats readAllShreddedNoReconstruction(
      org.apache.hadoop.fs.Path file, Configuration conf, Fixture fixture) throws IOException {
    List<ColumnDescriptor> cols = new ArrayList<>();
    cols.add(fixture.metadataColumn);
    if (fixture.topValueColumn != null) {
      cols.add(fixture.topValueColumn);
    }
    for (String[] path : fixture.allShreddedTypedPaths) {
      cols.add(fixture.schema.getColumnDescription(path));
    }
    for (String[] path : fixture.allShreddedValuePaths) {
      cols.add(fixture.schema.getColumnDescription(path));
    }
    return readColumns(file, conf, fixture, cols, ReadMode.DRAIN);
  }

  private static ReadStats readWithReconstruction(
      org.apache.hadoop.fs.Path file, Configuration conf, Fixture fixture) throws IOException {
    List<ColumnDescriptor> cols = new ArrayList<>();
    cols.add(fixture.metadataColumn);
    if (fixture.topValueColumn != null) {
      cols.add(fixture.topValueColumn);
    }
    cols.add(fixture.probeTypedColumn);
    if (fixture.probeValueColumn != null) {
      cols.add(fixture.probeValueColumn);
    }
    return readColumns(file, conf, fixture, cols, ReadMode.RECONSTRUCT);
  }

  private static ReadStats readWithSparkReserialize(
      org.apache.hadoop.fs.Path file, Configuration conf, Fixture fixture) throws IOException {
    List<ColumnDescriptor> cols = new ArrayList<>();
    cols.add(fixture.metadataColumn);
    if (fixture.topValueColumn != null) {
      cols.add(fixture.topValueColumn);
    }
    for (String[] path : fixture.allShreddedTypedPaths) {
      cols.add(fixture.schema.getColumnDescription(path));
    }
    for (String[] path : fixture.allShreddedValuePaths) {
      cols.add(fixture.schema.getColumnDescription(path));
    }
    return readColumns(file, conf, fixture, cols, ReadMode.SPARK_RESERIALIZE);
  }

  private enum ReadMode {
    DRAIN,
    RECONSTRUCT,
    SPARK_RESERIALIZE,
    PROJECT_N_FIELDS,
    FILTER_PROJECT_N_FIELDS,
    AGG_SUM_TYPED
  }

  /**
   * Resolve a comma-separated list of top-level field names to {@link ShreddedFieldCol}
   * descriptors. Skips fields that are not primitive-shredded at the top level (quiet skip - user
   * gets fewer fields).
   */
  private static List<ShreddedFieldCol> resolveProjectFields(Fixture fixture, String csv) {
    List<ShreddedFieldCol> resolved = new ArrayList<>();
    if (csv == null || csv.isEmpty()) {
      return resolved;
    }
    for (String rawName : csv.split(",", -1)) {
      String name = rawName.trim();
      if (name.isEmpty()) {
        continue;
      }
      String[] typedPath = null;
      for (String[] path : fixture.allShreddedTypedPaths) {
        // top-level primitive: [variant, "typed_value", name, "typed_value"]
        if (path.length == 4 && name.equals(path[2])) {
          typedPath = path;
          break;
        }
      }
      if (typedPath == null) {
        System.err.println(
            "project_n_fields: skipping '" + name + "' - not a top-level primitive shred");
        continue;
      }
      ColumnDescriptor typedDesc = fixture.schema.getColumnDescription(typedPath);
      ShreddedFieldCol sfc = new ShreddedFieldCol();
      sfc.name = name;
      sfc.typedMaxDef = typedDesc.getMaxDefinitionLevel();
      sfc.typedType = typedDesc.getPrimitiveType().getPrimitiveTypeName();
      Type parquetField = fixture.schema.getType(typedPath);
      sfc.isStringLogical =
          parquetField != null
              && parquetField.getLogicalTypeAnnotation()
                  instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation;
      // We defer typedReader assignment - it needs a fresh reader per row group.
      // Stashing the descriptor in the name-only slot is enough; per-rowgroup setup happens in
      // drain.
      resolved.add(sfc);
    }
    return resolved;
  }

  /**
   * Resolve a single top-level primitive shredded field by name. Returns null if not shreddable.
   */
  private static ShreddedFieldCol resolveOneField(Fixture fixture, String name) {
    if (name == null || name.isEmpty()) {
      return null;
    }
    List<ShreddedFieldCol> single = resolveProjectFields(fixture, name);
    return single.isEmpty() ? null : single.get(0);
  }

  /**
   * Find the first top-level primitive shredded field whose typed_value is BINARY with a
   * StringLogicalTypeAnnotation, so filter benchmarks can also run against a string field (not just
   * the default numeric filter). Returns null if none exist.
   */
  private static ShreddedFieldCol findFirstStringField(Fixture fixture) {
    for (String[] path : fixture.allShreddedTypedPaths) {
      if (path.length != 4) continue;
      ColumnDescriptor desc = fixture.schema.getColumnDescription(path);
      if (desc.getPrimitiveType().getPrimitiveTypeName()
          != PrimitiveType.PrimitiveTypeName.BINARY) {
        continue;
      }
      Type parquetField = fixture.schema.getType(path);
      if (parquetField != null
          && parquetField.getLogicalTypeAnnotation()
              instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
        ShreddedFieldCol sfc = new ShreddedFieldCol();
        sfc.name = path[2];
        sfc.typedMaxDef = desc.getMaxDefinitionLevel();
        sfc.typedType = desc.getPrimitiveType().getPrimitiveTypeName();
        sfc.isStringLogical = true;
        return sfc;
      }
    }
    return null;
  }

  /**
   * Path F: filter + project N fields. Ryan's exact sync ask (2026-07-02): "generate some subset of
   * rows that you are projecting, right? Have some, you know, filter. And run the filter, project
   * the rows with some selectivity, and then have a black hole consume that ... internal row that
   * you just reconstructed." Reads the filter column, applies a >= predicate against
   * filterMinValue, and for matching rows decodes each project field. Everything sinks into
   * BLACKHOLE. No VariantVal, no Spark.
   */
  private static ReadStats readFilterProjectNFields(
      org.apache.hadoop.fs.Path file, Configuration conf, Fixture fixture) throws IOException {
    if (fixture.filterField == null || fixture.projectFields.isEmpty()) {
      return new ReadStats();
    }
    // Populate filterMinValue for this (field, selectivityPct) combo via a one-shot pre-scan of the
    // filter column. Cached across timed runs so the pre-scan doesn't get double-counted.
    String cacheKey = fixture.filterField.name + "@" + fixture.filterSelectivityPct;
    Long cached = fixture.filterThresholdCache.get(cacheKey);
    if (cached == null) {
      cached = computeFilterThreshold(file, conf, fixture);
      fixture.filterThresholdCache.put(cacheKey, cached);
    }
    fixture.filterMinValue = cached;

    List<ColumnDescriptor> cols = new ArrayList<>();
    cols.add(
        fixture.schema.getColumnDescription(
            new String[] {
              fixture.variantColumn, "typed_value", fixture.filterField.name, "typed_value"
            }));
    for (ShreddedFieldCol sfc : fixture.projectFields) {
      cols.add(
          fixture.schema.getColumnDescription(
              new String[] {fixture.variantColumn, "typed_value", sfc.name, "typed_value"}));
    }
    return readColumns(file, conf, fixture, cols, ReadMode.FILTER_PROJECT_N_FIELDS);
  }

  /**
   * Pre-scan the filter column, sort the sample, and pick the threshold at position (100 -
   * selectivityPct)%. So selectivityPct=10 -> threshold at 90th percentile -> ~10% of rows pass the
   * >= predicate. Deterministic and matches the requested selectivity to within one percentile step
   * per file.
   */
  @SuppressWarnings("deprecation")
  private static long computeFilterThreshold(
      org.apache.hadoop.fs.Path file, Configuration conf, Fixture fixture) throws IOException {
    ColumnDescriptor filterDesc =
        fixture.schema.getColumnDescription(
            new String[] {
              fixture.variantColumn, "typed_value", fixture.filterField.name, "typed_value"
            });
    PrimitiveType.PrimitiveTypeName type = filterDesc.getPrimitiveType().getPrimitiveTypeName();
    // Sorting non-integer types via raw bits or hashCode yields a meaningless order (e.g.,
    // Float.floatToRawIntBits sorts negatives above positives; BINARY hashCode is not
    // lexicographic). Rather than publish a fake selectivity, skip the sweep for those types
    // - the caller's downstream drain still runs, comparing against Long.MIN_VALUE (all pass).
    if (type != PrimitiveType.PrimitiveTypeName.INT32
        && type != PrimitiveType.PrimitiveTypeName.INT64
        && type != PrimitiveType.PrimitiveTypeName.BOOLEAN) {
      System.err.println(
          "[BENCH] selectivity sweep skipped for type="
              + type
              + " field="
              + fixture.filterField.name
              + " (proper typed sort not implemented; using ~100% pass-through)");
      return Long.MIN_VALUE;
    }
    List<Long> vals = new ArrayList<>();
    int maxDef = filterDesc.getMaxDefinitionLevel();
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(file, conf))) {
      reader.setRequestedSchema(
          projectionOf(fixture.schema, Collections.singletonList(filterDesc)));
      PageReadStore rg;
      while ((rg = reader.readNextRowGroup()) != null) {
        ColumnReadStoreImpl store =
            new ColumnReadStoreImpl(rg, buildConverter(fixture.schema), fixture.schema, null);
        ColumnReader col = store.getColumnReader(filterDesc);
        long total = col.getTotalValueCount();
        for (long i = 0; i < total; i++) {
          if (col.getCurrentDefinitionLevel() == maxDef) {
            switch (type) {
              case INT32:
                vals.add((long) col.getInteger());
                break;
              case INT64:
                vals.add(col.getLong());
                break;
              case BOOLEAN:
                vals.add(col.getBoolean() ? 1L : 0L);
                break;
              default:
                throw new IllegalStateException("unsupported filter type: " + type);
            }
          }
          col.consume();
        }
      }
    }
    if (vals.isEmpty()) {
      return Long.MIN_VALUE; // all rows pass a MIN_VALUE >= predicate
    }
    long[] arr = new long[vals.size()];
    for (int i = 0; i < arr.length; i++) arr[i] = vals.get(i);
    java.util.Arrays.sort(arr);
    // selectivityPct% pass -> threshold at (100 - selectivityPct)%
    int idx = (int) Math.floor(arr.length * (100 - fixture.filterSelectivityPct) / 100.0);
    if (idx >= arr.length) idx = arr.length - 1;
    if (idx < 0) idx = 0;
    return arr[idx];
  }

  /**
   * Path G: SUM aggregate over a single typed column. Mimics the Spark-free ideal for SELECT
   * SUM(variant_get(payload, '$.<aggField>', 'long')) FROM t
   */
  private static ReadStats readAggSumTyped(
      org.apache.hadoop.fs.Path file, Configuration conf, Fixture fixture) throws IOException {
    if (fixture.aggField == null) {
      return new ReadStats();
    }
    List<ColumnDescriptor> cols = new ArrayList<>();
    cols.add(
        fixture.schema.getColumnDescription(
            new String[] {
              fixture.variantColumn, "typed_value", fixture.aggField.name, "typed_value"
            }));
    return readColumns(file, conf, fixture, cols, ReadMode.AGG_SUM_TYPED);
  }

  @SuppressWarnings("deprecation")
  private static long drainFilterProjectNFields(
      ColumnReadStoreImpl store, Fixture fixture, ReadStats stats) {
    // Cache readers by column path. ColumnReadStoreImpl.getColumnReader creates a fresh
    // ColumnReaderImpl each call; the first call drains the underlying PageReader's page queue,
    // so a second call for the SAME descriptor NPEs on the next readPage(). When the filter
    // column also appears in the project list (a common config), this bug would fire without
    // sharing readers.
    java.util.Map<String, ColumnReader> readerCache = new java.util.HashMap<>();

    // Set up filter reader.
    String[] filterPath =
        new String[] {
          fixture.variantColumn, "typed_value", fixture.filterField.name, "typed_value"
        };
    ColumnDescriptor filterDesc = fixture.schema.getColumnDescription(filterPath);
    String filterKey = String.join(".", filterPath);
    ColumnReader filterReader = store.getColumnReader(filterDesc);
    readerCache.put(filterKey, filterReader);
    int filterMaxDef = filterDesc.getMaxDefinitionLevel();
    PrimitiveType.PrimitiveTypeName filterType =
        filterDesc.getPrimitiveType().getPrimitiveTypeName();

    // Set up project readers, sharing with the filter reader when a project field IS the filter.
    // Track which project entries share the filter reader so we don't double-consume that column.
    List<ShreddedFieldCol> projectCols = new ArrayList<>();
    java.util.Set<Integer> projectSharedWithFilter = new java.util.HashSet<>();
    long total = filterReader.getTotalValueCount();
    for (int i = 0; i < fixture.projectFields.size(); i++) {
      ShreddedFieldCol template = fixture.projectFields.get(i);
      ShreddedFieldCol sfc = new ShreddedFieldCol();
      sfc.name = template.name;
      sfc.typedMaxDef = template.typedMaxDef;
      sfc.typedType = template.typedType;
      sfc.isStringLogical = template.isStringLogical;
      String[] path =
          new String[] {fixture.variantColumn, "typed_value", template.name, "typed_value"};
      String key = String.join(".", path);
      ColumnReader cached = readerCache.get(key);
      if (cached != null) {
        sfc.typedReader = cached;
        if (key.equals(filterKey)) {
          projectSharedWithFilter.add(i);
        }
      } else {
        sfc.typedReader = store.getColumnReader(fixture.schema.getColumnDescription(path));
        readerCache.put(key, sfc.typedReader);
      }
      projectCols.add(sfc);
    }

    long sink = 0;
    long matched = 0;
    for (long row = 0; row < total; row++) {
      // Decode filter value and test predicate.
      boolean keep = false;
      long filterVal = 0;
      boolean filterDefined = filterReader.getCurrentDefinitionLevel() == filterMaxDef;
      if (filterDefined) {
        switch (filterType) {
          case INT32:
            filterVal = filterReader.getInteger();
            break;
          case INT64:
            filterVal = filterReader.getLong();
            break;
          case BOOLEAN:
            filterVal = filterReader.getBoolean() ? 1 : 0;
            break;
          case BINARY:
          case FIXED_LEN_BYTE_ARRAY:
          case INT96:
            filterVal = filterReader.getBinary().hashCode();
            break;
          case FLOAT:
            filterVal = Float.floatToRawIntBits(filterReader.getFloat());
            break;
          case DOUBLE:
            filterVal = Double.doubleToRawLongBits(filterReader.getDouble());
            break;
          default:
            throw new IllegalStateException("unsupported filter type: " + filterType);
        }
        stats.valuesDecoded += 1;
        // Threshold was pre-computed by computeFilterThreshold at target selectivity %.
        keep = filterVal >= fixture.filterMinValue;
      }
      filterReader.consume();

      // For every project column, decode+sink when the filter matched; otherwise skip (still
      // consume). Columns that share the filter reader can't be re-decoded (getBinary/getInteger
      // is only valid ONCE per position, and filterReader.consume() already advanced past it),
      // so we fold the already-decoded filterVal into sink and skip the second consume.
      for (int i = 0; i < projectCols.size(); i++) {
        ShreddedFieldCol sfc = projectCols.get(i);
        boolean sharedWithFilter = projectSharedWithFilter.contains(i);
        if (sharedWithFilter) {
          // Reader already advanced by filter phase. Reuse the decoded value if the row matched.
          if (keep && filterDefined) {
            sink += filterVal;
            stats.valuesDecoded += 1;
          }
          continue;
        }
        if (keep && sfc.typedReader.getCurrentDefinitionLevel() == sfc.typedMaxDef) {
          switch (sfc.typedType) {
            case INT32:
              sink += sfc.typedReader.getInteger();
              break;
            case INT64:
              sink += sfc.typedReader.getLong();
              break;
            case FLOAT:
              sink += Float.floatToRawIntBits(sfc.typedReader.getFloat());
              break;
            case DOUBLE:
              sink += Double.doubleToRawLongBits(sfc.typedReader.getDouble());
              break;
            case BOOLEAN:
              sink += sfc.typedReader.getBoolean() ? 1 : 0;
              break;
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
            case INT96:
              if (sfc.isStringLogical && sfc.typedType == PrimitiveType.PrimitiveTypeName.BINARY) {
                sink += sfc.typedReader.getBinary().toStringUsingUTF8().hashCode();
              } else {
                sink += sfc.typedReader.getBinary().length();
              }
              break;
            default:
              throw new IllegalStateException("unsupported project type: " + sfc.typedType);
          }
          stats.valuesDecoded += 1;
        }
        sfc.typedReader.consume();
      }
      if (keep) {
        matched += 1;
      }
    }
    // Fold matched count into sink so the JIT can't elide the branch.
    return sink ^ matched;
  }

  @SuppressWarnings("deprecation")
  private static long drainAggSumTyped(
      ColumnReadStoreImpl store, Fixture fixture, ReadStats stats) {
    String[] aggPath =
        new String[] {fixture.variantColumn, "typed_value", fixture.aggField.name, "typed_value"};
    ColumnDescriptor desc = fixture.schema.getColumnDescription(aggPath);
    ColumnReader reader = store.getColumnReader(desc);
    int maxDef = desc.getMaxDefinitionLevel();
    PrimitiveType.PrimitiveTypeName type = desc.getPrimitiveType().getPrimitiveTypeName();

    long total = reader.getTotalValueCount();
    long sum = 0;
    for (long row = 0; row < total; row++) {
      if (reader.getCurrentDefinitionLevel() == maxDef) {
        switch (type) {
          case INT32:
            sum += reader.getInteger();
            break;
          case INT64:
            sum += reader.getLong();
            break;
          case FLOAT:
            sum += Float.floatToRawIntBits(reader.getFloat());
            break;
          case DOUBLE:
            sum += Double.doubleToRawLongBits(reader.getDouble());
            break;
          case BOOLEAN:
            sum += reader.getBoolean() ? 1 : 0;
            break;
          case BINARY:
          case FIXED_LEN_BYTE_ARRAY:
          case INT96:
            sum += reader.getBinary().length();
            break;
          default:
            throw new IllegalStateException("unsupported agg type: " + type);
        }
        stats.valuesDecoded += 1;
      }
      reader.consume();
    }
    return sum;
  }

  /**
   * Path H: Snowflake Q6/Q7 reader-only - read first element of a shredded array column ($[0]
   * access). Q6 = numeric first element, Q7 = string first element. Uses the arrays_shredded
   * fixture path if provided. Skipped when arraysFixturePath is null. The arrays_shredded schema
   * (per SparkBenchDriver + DataGenerator SCHEMA_ARRAYS) has top-level columns arr_number
   * (list<int>), arr_text (list<string>), arr_graph (list<list<int>>). When arrays are shredded,
   * list.element gets its own typed_value column.
   */
  @SuppressWarnings("deprecation")
  private static ReadStats readArraysFirstElement(
      Configuration conf, Fixture fixture, boolean isString) throws IOException {
    ReadStats stats = new ReadStats();
    if (fixture.arraysFixturePath == null) {
      return stats;
    }
    org.apache.hadoop.fs.Path arraysPath = new org.apache.hadoop.fs.Path(fixture.arraysFixturePath);
    try (ParquetFileReader reader =
        ParquetFileReader.open(HadoopInputFile.fromPath(arraysPath, conf))) {
      MessageType arraysSchema = reader.getFooter().getFileMetaData().getSchema();
      // arr_number / arr_text are VARIANT columns per SparkBenchDriver:191-207. When shredded,
      // Iceberg emits: <col> GROUP { metadata, value, typed_value GROUP (LIST) { list REPEATED {
      // element REQUIRED GROUP { value, typed_value <primitive> } } } }.
      // Find the primitive leaf by walking every column in the schema and matching the top-level
      // name + the presence of "list" + "element" + "typed_value" in the path.
      String topName = isString ? "arr_text" : "arr_number";
      ColumnDescriptor elementCol = null;
      for (ColumnDescriptor cd : arraysSchema.getColumns()) {
        String[] p = cd.getPath();
        if (p.length < 2 || !topName.equals(p[0])) {
          continue;
        }
        // Prefer the primitive leaf under list.element.typed_value.
        boolean hasList = false;
        boolean hasElement = false;
        boolean endsInTypedValue = "typed_value".equals(p[p.length - 1]);
        for (String seg : p) {
          if ("list".equals(seg)) hasList = true;
          if ("element".equals(seg)) hasElement = true;
        }
        if (hasList && hasElement && endsInTypedValue) {
          elementCol = cd;
          break;
        }
      }
      // Fallback: any primitive column under the top-level name (e.g. metadata for reference).
      if (elementCol == null) {
        for (ColumnDescriptor cd : arraysSchema.getColumns()) {
          if (cd.getPath().length > 0 && topName.equals(cd.getPath()[0])) {
            elementCol = cd;
            break;
          }
        }
      }
      if (elementCol == null) {
        return stats;
      }
      // Count column chunk bytes for reporting.
      for (BlockMetaData block : reader.getRowGroups()) {
        for (ColumnChunkMetaData chunk : block.getColumns()) {
          if (chunk.getPath().equals(ColumnPath.get(elementCol.getPath()))) {
            stats.compressedBytes += chunk.getTotalSize();
            stats.uncompressedBytes += chunk.getTotalUncompressedSize();
            stats.columnChunksRead += 1;
          }
        }
      }
      reader.setRequestedSchema(projectionOf(arraysSchema, Collections.singletonList(elementCol)));
      PageReadStore rowGroup;
      long sink = 0;
      int elementMaxDef = elementCol.getMaxDefinitionLevel();
      PrimitiveType.PrimitiveTypeName type = elementCol.getPrimitiveType().getPrimitiveTypeName();
      while ((rowGroup = reader.readNextRowGroup()) != null) {
        stats.rowGroupsRead += 1;
        stats.rowsRead += rowGroup.getRowCount();
        ColumnReadStoreImpl store =
            new ColumnReadStoreImpl(rowGroup, buildConverter(arraysSchema), arraysSchema, null);
        ColumnReader col = store.getColumnReader(elementCol);
        long total = col.getTotalValueCount();
        for (long i = 0; i < total; i++) {
          // First-element access: only sink when repetition level is 0 (start of a new list).
          if (col.getCurrentRepetitionLevel() == 0
              && col.getCurrentDefinitionLevel() == elementMaxDef) {
            switch (type) {
              case INT32:
                sink += col.getInteger();
                break;
              case INT64:
                sink += col.getLong();
                break;
              case BINARY:
              case FIXED_LEN_BYTE_ARRAY:
              case INT96:
                sink += col.getBinary().length();
                break;
              default:
                sink += 1;
                break;
            }
            stats.valuesDecoded += 1;
          }
          col.consume();
        }
        BLACKHOLE ^= sink;
      }
    }
    return stats;
  }

  /**
   * Path E: project N fields directly from shredded typed_value columns into native Java values.
   * Simulates the ideal Spark-pushdown case where variant_get() returns typed values without ever
   * constructing a full Variant or VariantVal (mirrors Qiegang's SparkVariantExtractionReaders
   * inlineConvert path at spark/v4.1/.../SparkVariantExtractionReaders.java:294-348).
   */
  private static ReadStats readProjectNFields(
      org.apache.hadoop.fs.Path file, Configuration conf, Fixture fixture) throws IOException {
    if (fixture.projectFields.isEmpty()) {
      // no fields to project -> report zero-cost measurement
      return new ReadStats();
    }
    List<ColumnDescriptor> cols = new ArrayList<>();
    for (ShreddedFieldCol sfc : fixture.projectFields) {
      // top-level primitive path
      String[] path = new String[] {fixture.variantColumn, "typed_value", sfc.name, "typed_value"};
      cols.add(fixture.schema.getColumnDescription(path));
    }
    return readColumns(file, conf, fixture, cols, ReadMode.PROJECT_N_FIELDS);
  }

  @SuppressWarnings("deprecation")
  private static long drainProjectNFields(
      ColumnReadStoreImpl store, Fixture fixture, ReadStats stats) {
    List<ShreddedFieldCol> fields = new ArrayList<>();
    long total = -1;
    for (ShreddedFieldCol template : fixture.projectFields) {
      ShreddedFieldCol sfc = new ShreddedFieldCol();
      sfc.name = template.name;
      sfc.typedMaxDef = template.typedMaxDef;
      sfc.typedType = template.typedType;
      sfc.isStringLogical = template.isStringLogical;
      String[] path =
          new String[] {fixture.variantColumn, "typed_value", template.name, "typed_value"};
      ColumnDescriptor desc = fixture.schema.getColumnDescription(path);
      sfc.typedReader = store.getColumnReader(desc);
      if (total < 0) {
        total = sfc.typedReader.getTotalValueCount();
      }
      fields.add(sfc);
    }
    if (total < 0) {
      return 0;
    }

    long sink = 0;
    for (long row = 0; row < total; row++) {
      for (ShreddedFieldCol sfc : fields) {
        if (sfc.typedReader.getCurrentDefinitionLevel() == sfc.typedMaxDef) {
          switch (sfc.typedType) {
            case INT32:
              sink += sfc.typedReader.getInteger();
              break;
            case INT64:
              sink += sfc.typedReader.getLong();
              break;
            case FLOAT:
              sink += Float.floatToRawIntBits(sfc.typedReader.getFloat());
              break;
            case DOUBLE:
              sink += Double.doubleToRawLongBits(sfc.typedReader.getDouble());
              break;
            case BOOLEAN:
              sink += sfc.typedReader.getBoolean() ? 1 : 0;
              break;
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
            case INT96:
              if (sfc.isStringLogical && sfc.typedType == PrimitiveType.PrimitiveTypeName.BINARY) {
                sink += sfc.typedReader.getBinary().toStringUsingUTF8().hashCode();
              } else {
                sink += sfc.typedReader.getBinary().length();
              }
              break;
            default:
              throw new IllegalStateException("unsupported: " + sfc.typedType);
          }
          stats.valuesDecoded += 1;
        }
        sfc.typedReader.consume();
      }
    }
    return sink;
  }

  private static ReadStats readColumns(
      org.apache.hadoop.fs.Path file,
      Configuration conf,
      Fixture fixture,
      List<ColumnDescriptor> requested,
      ReadMode mode)
      throws IOException {
    ReadStats stats = new ReadStats();
    MessageType projection = projectionOf(fixture.schema, requested);
    // Precompute the requested column-chunk paths so the bytes-read scan only credits chunks
    // in the projection, never the whole fixture.blocks. O(1) lookup + prevents accidental
    // double-counting if requested contains duplicates.
    Set<ColumnPath> requestedPaths = new LinkedHashSet<>();
    for (ColumnDescriptor col : requested) {
      requestedPaths.add(ColumnPath.get(col.getPath()));
    }
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(file, conf))) {
      reader.setRequestedSchema(projection);
      // bytes-read accounting from footer - only chunks in requestedPaths
      for (BlockMetaData block : fixture.blocks) {
        for (ColumnChunkMetaData chunk : block.getColumns()) {
          if (requestedPaths.contains(chunk.getPath())) {
            stats.compressedBytes += chunk.getTotalSize();
            stats.uncompressedBytes += chunk.getTotalUncompressedSize();
            stats.columnChunksRead += 1;
          }
        }
      }
      PageReadStore rowGroup;
      while ((rowGroup = reader.readNextRowGroup()) != null) {
        stats.rowGroupsRead += 1;
        stats.rowsRead += rowGroup.getRowCount();
        ColumnReadStoreImpl store =
            new ColumnReadStoreImpl(rowGroup, buildConverter(projection), projection, null);
        switch (mode) {
          case DRAIN:
            for (ColumnDescriptor c : requested) {
              BLACKHOLE ^= drainColumn(store.getColumnReader(c), stats);
            }
            break;
          case RECONSTRUCT:
            BLACKHOLE = drainWithReconstruction(store, fixture, stats);
            break;
          case SPARK_RESERIALIZE:
            BLACKHOLE = drainWithSparkReserialize(store, fixture, stats);
            break;
          case PROJECT_N_FIELDS:
            BLACKHOLE = drainProjectNFields(store, fixture, stats);
            break;
          case FILTER_PROJECT_N_FIELDS:
            BLACKHOLE = drainFilterProjectNFields(store, fixture, stats);
            break;
          case AGG_SUM_TYPED:
            BLACKHOLE = drainAggSumTyped(store, fixture, stats);
            break;
          default:
            throw new IllegalStateException("unsupported mode: " + mode);
        }
      }
    }
    return stats;
  }

  @SuppressWarnings("deprecation")
  private static long drainColumn(ColumnReader cr, ReadStats stats) {
    long total = cr.getTotalValueCount();
    int maxDef = cr.getDescriptor().getMaxDefinitionLevel();
    PrimitiveType.PrimitiveTypeName type =
        cr.getDescriptor().getPrimitiveType().getPrimitiveTypeName();
    long sink = 0;
    for (long i = 0; i < total; i++) {
      if (cr.getCurrentDefinitionLevel() == maxDef) {
        switch (type) {
          case INT32:
            sink += cr.getInteger();
            break;
          case INT64:
            sink += cr.getLong();
            break;
          case FLOAT:
            sink += Float.floatToRawIntBits(cr.getFloat());
            break;
          case DOUBLE:
            sink += Double.doubleToRawLongBits(cr.getDouble());
            break;
          case BOOLEAN:
            sink += cr.getBoolean() ? 1 : 0;
            break;
          case BINARY:
          case FIXED_LEN_BYTE_ARRAY:
          case INT96:
            sink += cr.getBinary().length();
            break;
          default:
            throw new IllegalStateException("unsupported: " + type);
        }
        stats.valuesDecoded += 1;
      }
      cr.consume();
    }
    return sink;
  }

  @SuppressWarnings("deprecation")
  private static long drainWithReconstruction(
      ColumnReadStoreImpl store, Fixture fixture, ReadStats stats) {
    ColumnReader meta = store.getColumnReader(fixture.metadataColumn);
    ColumnReader topValue =
        fixture.topValueColumn == null ? null : store.getColumnReader(fixture.topValueColumn);
    ColumnReader typedProbe = store.getColumnReader(fixture.probeTypedColumn);
    ColumnReader valueProbe =
        fixture.probeValueColumn == null ? null : store.getColumnReader(fixture.probeValueColumn);

    long total = meta.getTotalValueCount();
    int metaMaxDef = fixture.metadataColumn.getMaxDefinitionLevel();
    int topValueMaxDef =
        fixture.topValueColumn == null ? -1 : fixture.topValueColumn.getMaxDefinitionLevel();
    int typedMaxDef = fixture.probeTypedColumn.getMaxDefinitionLevel();
    int valueMaxDef =
        fixture.probeValueColumn == null ? -1 : fixture.probeValueColumn.getMaxDefinitionLevel();
    PrimitiveType.PrimitiveTypeName typedType =
        fixture.probeTypedColumn.getPrimitiveType().getPrimitiveTypeName();

    long sink = 0;
    for (long i = 0; i < total; i++) {
      ByteBuffer metaBytes = null;
      if (meta.getCurrentDefinitionLevel() == metaMaxDef) {
        metaBytes = meta.getBinary().toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
        stats.valuesDecoded += 1;
      }
      meta.consume();

      if (topValue != null) {
        if (topValue.getCurrentDefinitionLevel() == topValueMaxDef) {
          topValue.getBinary();
          stats.valuesDecoded += 1;
        }
        topValue.consume();
      }

      boolean typedPresent = typedProbe.getCurrentDefinitionLevel() == typedMaxDef;
      if (typedPresent) {
        switch (typedType) {
          case INT32:
            sink += typedProbe.getInteger();
            break;
          case INT64:
            sink += typedProbe.getLong();
            break;
          case FLOAT:
            sink += Float.floatToRawIntBits(typedProbe.getFloat());
            break;
          case DOUBLE:
            sink += Double.doubleToRawLongBits(typedProbe.getDouble());
            break;
          case BOOLEAN:
            sink += typedProbe.getBoolean() ? 1 : 0;
            break;
          case BINARY:
          case FIXED_LEN_BYTE_ARRAY:
          case INT96:
            sink += typedProbe.getBinary().length();
            break;
          default:
            throw new IllegalStateException("unsupported: " + typedType);
        }
        stats.valuesDecoded += 1;
      }
      typedProbe.consume();

      if (valueProbe != null) {
        boolean valuePresent = valueProbe.getCurrentDefinitionLevel() == valueMaxDef;
        if (valuePresent && !typedPresent && metaBytes != null) {
          ByteBuffer valueBytes =
              valueProbe.getBinary().toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
          VariantMetadata parsedMeta = VariantMetadata.from(metaBytes);
          VariantValue parsedValue = VariantValue.from(parsedMeta, valueBytes);
          Variant reconstructed = Variant.of(parsedMeta, parsedValue);
          sink += reconstructed.hashCode();
          stats.valuesDecoded += 1;
        } else if (valuePresent) {
          valueProbe.getBinary();
          stats.valuesDecoded += 1;
        }
        valueProbe.consume();
      }
    }
    return sink;
  }

  private static final class ShreddedFieldCol {
    String name; // leaf field name (last semantic segment)
    String parentKey; // dot-joined parent path ("" for top-level primitives)
    ColumnReader typedReader;
    int typedMaxDef;
    PrimitiveType.PrimitiveTypeName typedType;
    boolean isStringLogical;
    ColumnReader valueReader;
    int valueMaxDef;
  }

  @SuppressWarnings("deprecation")
  private static long drainWithSparkReserialize(
      ColumnReadStoreImpl store, Fixture fixture, ReadStats stats) {
    ColumnReader meta = store.getColumnReader(fixture.metadataColumn);
    ColumnReader topValue =
        fixture.topValueColumn == null ? null : store.getColumnReader(fixture.topValueColumn);
    int metaMaxDef = fixture.metadataColumn.getMaxDefinitionLevel();
    int topValueMaxDef =
        fixture.topValueColumn == null ? -1 : fixture.topValueColumn.getMaxDefinitionLevel();

    // Collect ALL primitive shredded leaves at any depth. Each carries its parent path
    // (dot-joined semantic path, skipping "typed_value" wrappers and the top-level variant column).
    List<ShreddedFieldCol> leaves = new ArrayList<>();
    for (int idx = 0; idx < fixture.allShreddedTypedPaths.size(); idx++) {
      String[] typedPath = fixture.allShreddedTypedPaths.get(idx);
      // Build semantic path from the raw Parquet path, e.g.:
      //   [payload, typed_value, actor, typed_value, avatar_url, typed_value]
      //   -> semantic [actor, avatar_url]
      //   -> parentKey "actor", leafName "avatar_url"
      List<String> semantic = new ArrayList<>();
      for (int i = 1; i < typedPath.length; i++) {
        if (!"typed_value".equals(typedPath[i])) {
          semantic.add(typedPath[i]);
        }
      }
      if (semantic.isEmpty()) {
        continue;
      }
      String leafName = semantic.remove(semantic.size() - 1);
      String parentKey = String.join(".", semantic);

      ColumnDescriptor typedDesc = fixture.schema.getColumnDescription(typedPath);
      ShreddedFieldCol sfc = new ShreddedFieldCol();
      sfc.name = leafName;
      sfc.parentKey = parentKey;
      sfc.typedReader = store.getColumnReader(typedDesc);
      sfc.typedMaxDef = typedDesc.getMaxDefinitionLevel();
      sfc.typedType = typedDesc.getPrimitiveType().getPrimitiveTypeName();
      Type parquetField = fixture.schema.getType(typedPath);
      sfc.isStringLogical =
          parquetField != null
              && parquetField.getLogicalTypeAnnotation()
                  instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation;

      // Match sibling value column by same parent path.
      String[] siblingValuePath = new String[typedPath.length];
      System.arraycopy(typedPath, 0, siblingValuePath, 0, typedPath.length - 1);
      siblingValuePath[typedPath.length - 1] = "value";
      for (String[] vp : fixture.allShreddedValuePaths) {
        if (java.util.Arrays.equals(vp, siblingValuePath)) {
          ColumnDescriptor valueDesc = fixture.schema.getColumnDescription(vp);
          sfc.valueReader = store.getColumnReader(valueDesc);
          sfc.valueMaxDef = valueDesc.getMaxDefinitionLevel();
          break;
        }
      }
      leaves.add(sfc);
    }

    // Precompute unique parent keys in deepest-first order so nested ShreddedObjects
    // are built before being placed into their parents.
    Set<String> parentSet = new LinkedHashSet<>();
    for (ShreddedFieldCol sfc : leaves) {
      // Include the parent AND all ancestors (empty parents may exist that hold only child
      // objects).
      String key = sfc.parentKey;
      while (true) {
        parentSet.add(key);
        if (key.isEmpty()) {
          break;
        }
        int lastDot = key.lastIndexOf('.');
        key = lastDot < 0 ? "" : key.substring(0, lastDot);
      }
    }
    List<String> parentsDeepestFirst = new ArrayList<>(parentSet);
    parentsDeepestFirst.sort((a, b) -> Integer.compare(depthOf(b), depthOf(a))); // desc by depth

    long total = meta.getTotalValueCount();
    long sink = 0;
    for (long row = 0; row < total; row++) {
      ByteBuffer metaBytes = null;
      if (meta.getCurrentDefinitionLevel() == metaMaxDef) {
        metaBytes = meta.getBinary().toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
        stats.valuesDecoded += 1;
      }
      meta.consume();

      VariantValue residual = null;
      VariantMetadata parsedMeta = metaBytes == null ? null : VariantMetadata.from(metaBytes);
      if (topValue != null) {
        if (topValue.getCurrentDefinitionLevel() == topValueMaxDef && parsedMeta != null) {
          ByteBuffer tvBytes = topValue.getBinary().toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
          residual = VariantValue.from(parsedMeta, tvBytes);
          stats.valuesDecoded += 1;
        }
        topValue.consume();
      }

      // Bottom-up build: deepest ShreddedObjects first, so parents can put child objects.
      Map<String, ShreddedObject> objects = new LinkedHashMap<>();
      if (parsedMeta != null) {
        for (String parentKey : parentsDeepestFirst) {
          ShreddedObject obj;
          if (parentKey.isEmpty() && residual instanceof VariantObject residualObj) {
            obj = Variants.object(parsedMeta, residualObj);
          } else {
            obj = Variants.object(parsedMeta);
          }
          objects.put(parentKey, obj);
        }
      }

      // Read every leaf and put into its direct parent ShreddedObject.
      for (ShreddedFieldCol sfc : leaves) {
        boolean typedPresent = sfc.typedReader.getCurrentDefinitionLevel() == sfc.typedMaxDef;
        boolean valuePresent =
            sfc.valueReader != null
                && sfc.valueReader.getCurrentDefinitionLevel() == sfc.valueMaxDef;
        ShreddedObject parentObj = objects.get(sfc.parentKey);
        if (typedPresent && parentObj != null) {
          VariantValue wrapper;
          switch (sfc.typedType) {
            case INT32:
              wrapper = Variants.of(sfc.typedReader.getInteger());
              break;
            case INT64:
              wrapper = Variants.of(sfc.typedReader.getLong());
              break;
            case FLOAT:
              wrapper = Variants.of(sfc.typedReader.getFloat());
              break;
            case DOUBLE:
              wrapper = Variants.of(sfc.typedReader.getDouble());
              break;
            case BOOLEAN:
              wrapper = Variants.of(sfc.typedReader.getBoolean());
              break;
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
            case INT96:
              if (sfc.isStringLogical && sfc.typedType == PrimitiveType.PrimitiveTypeName.BINARY) {
                wrapper = Variants.of(sfc.typedReader.getBinary().toStringUsingUTF8());
              } else {
                wrapper =
                    Variants.of(
                        sfc.typedReader.getBinary().toByteBuffer().order(ByteOrder.LITTLE_ENDIAN));
              }
              break;
            default:
              throw new IllegalStateException("unsupported: " + sfc.typedType);
          }
          // Skip fields whose name isn't in this row's metadata dictionary. That happens when the
          // shredded schema is a union across rows but a particular row's variant metadata only
          // holds the field names it actually contains.
          if (parsedMeta.id(sfc.name) >= 0) {
            parentObj.put(sfc.name, wrapper);
          }
          stats.valuesDecoded += 1;
        } else if (valuePresent && parentObj != null && parsedMeta != null) {
          ByteBuffer fbBytes =
              sfc.valueReader.getBinary().toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
          if (parsedMeta.id(sfc.name) >= 0) {
            parentObj.put(sfc.name, VariantValue.from(parsedMeta, fbBytes));
          }
          stats.valuesDecoded += 1;
        }
        sfc.typedReader.consume();
        if (sfc.valueReader != null) {
          sfc.valueReader.consume();
        }
      }

      // Attach child ShreddedObjects to their immediate parent.
      if (parsedMeta != null) {
        for (Map.Entry<String, ShreddedObject> entry : objects.entrySet()) {
          String childKey = entry.getKey();
          if (childKey.isEmpty()) {
            continue;
          }
          int lastDot = childKey.lastIndexOf('.');
          String parentKey = lastDot < 0 ? "" : childKey.substring(0, lastDot);
          String childName = lastDot < 0 ? childKey : childKey.substring(lastDot + 1);
          ShreddedObject parentObj = objects.get(parentKey);
          if (parentObj != null && parsedMeta.id(childName) >= 0) {
            parentObj.put(childName, entry.getValue());
          }
        }
      }

      ShreddedObject root = parsedMeta == null ? null : objects.get("");
      if (root == null && parsedMeta != null) {
        // No shredded leaves at any depth - fall back to an empty top-level.
        root = Variants.object(parsedMeta);
      }
      if (parsedMeta != null && root != null) {
        // this mimics SparkParquetReaders.VariantReader.read:526-537, but now the writeTo walk
        // recursively serializes the full nested ShreddedObject tree, not just top-level fields.
        Variant reconstructed = Variant.of(parsedMeta, root);
        byte[] mb = new byte[reconstructed.metadata().sizeInBytes()];
        reconstructed.metadata().writeTo(ByteBuffer.wrap(mb).order(ByteOrder.LITTLE_ENDIAN), 0);
        byte[] vb = new byte[reconstructed.value().sizeInBytes()];
        reconstructed.value().writeTo(ByteBuffer.wrap(vb).order(ByteOrder.LITTLE_ENDIAN), 0);
        sink += mb.length + vb.length;
      }
    }
    return sink;
  }

  private static int depthOf(String parentKey) {
    if (parentKey.isEmpty()) {
      return 0;
    }
    int depth = 1;
    for (int i = 0; i < parentKey.length(); i++) {
      if (parentKey.charAt(i) == '.') {
        depth += 1;
      }
    }
    return depth;
  }

  private static MessageType projectionOf(MessageType schema, List<ColumnDescriptor> cols) {
    // Build a projection that contains each requested column path.
    List<Type> topLevel = new ArrayList<>();
    Map<String, GroupBuilder> topByName = new LinkedHashMap<>();
    for (ColumnDescriptor c : cols) {
      String[] path = c.getPath();
      GroupBuilder root = topByName.get(path[0]);
      if (root == null) {
        Type origTop = schema.getType(path[0]);
        root = new GroupBuilder(origTop);
        topByName.put(path[0], root);
      }
      root.addPath(schema.getType(path[0]), path, 0);
    }
    for (GroupBuilder gb : topByName.values()) {
      topLevel.add(gb.build());
    }
    return new MessageType(schema.getName(), topLevel);
  }

  private static final class GroupBuilder {
    final Type original;
    final Map<String, GroupBuilder> children = new LinkedHashMap<>();
    boolean isLeaf;

    GroupBuilder(Type original) {
      this.original = original;
    }

    void addPath(Type currentType, String[] path, int idx) {
      if (idx == path.length - 1) {
        isLeaf = true;
        return;
      }
      GroupType asGroup = (GroupType) currentType;
      String childName = path[idx + 1];
      GroupBuilder child = children.get(childName);
      if (child == null) {
        child = new GroupBuilder(asGroup.getType(childName));
        children.put(childName, child);
      }
      child.addPath(asGroup.getType(childName), path, idx + 1);
    }

    Type build() {
      if (isLeaf || children.isEmpty()) {
        return original;
      }
      GroupType asGroup = (GroupType) original;
      List<Type> newFields = new ArrayList<>();
      for (Map.Entry<String, GroupBuilder> entry : children.entrySet()) {
        newFields.add(entry.getValue().build());
      }
      return new GroupType(asGroup.getRepetition(), asGroup.getName(), newFields);
    }
  }

  private static final class NoopGroupConverter extends GroupConverter {
    private final GroupType group;

    NoopGroupConverter(GroupType group) {
      this.group = group;
    }

    @Override
    public void start() {}

    @Override
    public void end() {}

    @Override
    public Converter getConverter(int fieldIndex) {
      Type field = group.getType(fieldIndex);
      if (field.isPrimitive()) {
        return new PrimitiveConverter() {};
      }
      return new NoopGroupConverter((GroupType) field);
    }
  }

  private static GroupConverter buildConverter(MessageType projection) {
    return new NoopGroupConverter(projection);
  }

  // ---------------------------------------------------------------------------
  // timing

  private static final class TimingResult {
    final String name;
    final long[] nanos;
    final ReadStats stats;

    TimingResult(String name, long[] nanos, ReadStats stats) {
      this.name = name;
      this.nanos = nanos;
      this.stats = stats;
    }

    long min() {
      long[] c = nanos.clone();
      Arrays.sort(c);
      return c[0];
    }

    long median() {
      long[] c = nanos.clone();
      Arrays.sort(c);
      return c[c.length / 2];
    }

    long max() {
      long[] c = nanos.clone();
      Arrays.sort(c);
      return c[c.length - 1];
    }
  }

  // Iterations re-open the ParquetFileReader on each call, but after iteration 1 the OS page
  // cache warms and subsequent runs are effectively hot-cache reads. Median-of-N is the warm
  // number; min-of-N approximates the cold-cache lower bound (iteration 1 typically wins).
  // Both are published in the CSV so downstream analysis can pick the appropriate cell.
  private static TimingResult measure(String name, ReadPath path) throws IOException {
    for (int i = 0; i < WARMUP_RUNS; i++) {
      path.read();
    }
    long[] nanos = new long[MEASURED_RUNS];
    ReadStats last = null;
    for (int i = 0; i < MEASURED_RUNS; i++) {
      long start = System.nanoTime();
      last = path.read();
      nanos[i] = System.nanoTime() - start;
    }
    return new TimingResult(name, nanos, last);
  }

  // ---------------------------------------------------------------------------
  // output

  private static void writeTimingsCsv(Path csv, Fixture fixture, List<TimingResult> results)
      throws IOException {
    try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(csv, StandardCharsets.UTF_8))) {
      pw.println(
          "workload,strategy,file,variant_column,probe_field,file_size_bytes,total_rows,"
              + "row_group_count,path,run1_ns,run2_ns,run3_ns,run4_ns,run5_ns,"
              + "min_ns,median_ns,max_ns,min_ms,median_ms,max_ms,cold_ns,warm_ns,"
              + "values_decoded,rows_read,row_groups_read,column_chunks_read,"
              + "compressed_bytes,uncompressed_bytes");
      for (TimingResult r : results) {
        StringBuilder sb = new StringBuilder();
        sb.append(esc(fixture.workload)).append(',');
        sb.append(esc(fixture.strategy)).append(',');
        sb.append(esc(fixture.filePath)).append(',');
        sb.append(esc(fixture.variantColumn)).append(',');
        sb.append(esc(fixture.probeField)).append(',');
        sb.append(fixture.fileSizeBytes).append(',');
        sb.append(fixture.totalRows).append(',');
        sb.append(fixture.rowGroupCount).append(',');
        sb.append(esc(r.name)).append(',');
        for (int i = 0; i < MEASURED_RUNS; i++) {
          sb.append(r.nanos[i]).append(',');
        }
        sb.append(r.min()).append(',');
        sb.append(r.median()).append(',');
        sb.append(r.max()).append(',');
        sb.append(fmt(r.min() / 1e6)).append(',');
        sb.append(fmt(r.median() / 1e6)).append(',');
        sb.append(fmt(r.max() / 1e6)).append(',');
        // cold_ns: iteration 1 alone (approximates cold-cache read). warm_ns: median-of-N,
        // which is dominated by hot-cache reads once the OS page cache is warm.
        sb.append(r.nanos.length > 0 ? r.nanos[0] : 0L).append(',');
        sb.append(r.median()).append(',');
        sb.append(r.stats.valuesDecoded).append(',');
        sb.append(r.stats.rowsRead).append(',');
        sb.append(r.stats.rowGroupsRead).append(',');
        sb.append(r.stats.columnChunksRead).append(',');
        sb.append(r.stats.compressedBytes).append(',');
        sb.append(r.stats.uncompressedBytes);
        pw.println(sb);
      }
    }
  }

  private static void writeScoresCsv(Path csv, Fixture fixture, List<ColumnScore> scores)
      throws IOException {
    try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(csv, StandardCharsets.UTF_8))) {
      pw.println(
          "workload,strategy,file,variant_column,shredded_field,score,reason,"
              + "value_nulls,value_total,typed_nulls,typed_total,typed_has_stats");
      int aggregate = 0;
      for (ColumnScore cs : scores) {
        aggregate += cs.score;
        pw.println(
            esc(fixture.workload)
                + ","
                + esc(fixture.strategy)
                + ","
                + esc(fixture.filePath)
                + ","
                + esc(fixture.variantColumn)
                + ","
                + esc(cs.fieldName)
                + ","
                + cs.score
                + ","
                + esc(cs.reason)
                + ","
                + cs.valueNulls
                + ","
                + cs.valueTotal
                + ","
                + cs.typedNulls
                + ","
                + cs.typedTotal
                + ","
                + cs.typedHasStats);
      }
      pw.println(
          esc(fixture.workload)
              + ","
              + esc(fixture.strategy)
              + ","
              + esc(fixture.filePath)
              + ","
              + esc(fixture.variantColumn)
              + ",AGGREGATE,"
              + aggregate
              + ",sum,-,-,-,-,-");
    }
  }

  private static String esc(String value) {
    if (value == null) {
      return "";
    }
    if (value.contains(",") || value.contains("\"")) {
      return "\"" + value.replace("\"", "\"\"") + "\"";
    }
    return value;
  }

  private static String fmt(double d) {
    return String.format(Locale.ROOT, "%.3f", d);
  }

  private static void printSummary(
      Fixture fixture, List<TimingResult> results, List<ColumnScore> scores) {
    System.out.println();
    System.out.println("========== Variant Extraction Quick-Check ==========");
    System.out.println("file          : " + fixture.filePath);
    System.out.println("file size     : " + fixture.fileSizeBytes + " bytes");
    System.out.println("total rows    : " + fixture.totalRows);
    System.out.println("row groups    : " + fixture.rowGroupCount);
    System.out.println("variant col   : " + fixture.variantColumn);
    System.out.println("probe field   : " + fixture.probeField);
    System.out.println("shredded cols : " + fixture.allShreddedTypedPaths.size());
    System.out.println();
    System.out.println(
        "timings (median across " + MEASURED_RUNS + " runs, " + WARMUP_RUNS + " warmup):");
    for (TimingResult r : results) {
      System.out.printf(
          Locale.ROOT,
          "  %-32s median=%7.2f ms  min=%7.2f  max=%7.2f  bytes=%d  values=%d%n",
          r.name,
          r.median() / 1e6,
          r.min() / 1e6,
          r.max() / 1e6,
          r.stats.compressedBytes,
          r.stats.valuesDecoded);
    }
    // Look up medians by path name (dynamic path list; indices no longer safe).
    Map<String, Double> medianByName = new LinkedHashMap<>();
    for (TimingResult r : results) {
      medianByName.put(r.name, r.median() / 1e6);
    }
    Double typedMs = medianByName.get("typed_only");
    Double noReconMs = medianByName.get("all_shredded_no_reconstruction");
    Double withReconMs = medianByName.get("with_reconstruction");
    Double reserializeMs = medianByName.get("with_spark_reserialize");
    Double projectMs = medianByName.get("project_n_fields");
    if (typedMs != null && reserializeMs != null) {
      System.out.println();
      if (noReconMs != null) {
        System.out.printf(
            Locale.ROOT,
            "speedup typed_only vs all_shredded_no_reconstruction: %.2fx%n",
            noReconMs / typedMs);
      }
      if (withReconMs != null) {
        System.out.printf(
            Locale.ROOT,
            "speedup typed_only vs with_reconstruction           : %.2fx%n",
            withReconMs / typedMs);
      }
      System.out.printf(
          Locale.ROOT,
          "speedup typed_only vs with_spark_reserialize        : %.2fx%n",
          reserializeMs / typedMs);
      if (projectMs != null) {
        System.out.printf(
            Locale.ROOT,
            "speedup project_n_fields vs with_spark_reserialize  : %.2fx  <-- Qiegang-branch simulation%n",
            reserializeMs / projectMs);
      }
      // Emit filter+project speedups for whatever selectivity variants ran.
      for (Map.Entry<String, Double> e : medianByName.entrySet()) {
        if (e.getKey().startsWith("filter_project_n_fields")) {
          System.out.printf(
              Locale.ROOT,
              "speedup %-40s vs reserialize: %.2fx%n",
              e.getKey(),
              reserializeMs / e.getValue());
        }
      }
      Double aggMs = medianByName.get("agg_sum_typed");
      if (aggMs != null) {
        System.out.printf(
            Locale.ROOT,
            "speedup agg_sum_typed vs reserialize                : %.2fx  <-- SUM aggregate, no Spark%n",
            reserializeMs / aggMs);
      }
      // Arrays paths (when present)
      for (Map.Entry<String, Double> e : medianByName.entrySet()) {
        if (e.getKey().startsWith("arrays_")) {
          System.out.printf(Locale.ROOT, "  %s median: %7.2f ms%n", e.getKey(), e.getValue());
        }
      }
      if (noReconMs != null) {
        System.out.printf(
            Locale.ROOT,
            "spark reserialize overhead vs no_reconstruction     : %.2fx%n",
            reserializeMs / noReconMs);
      }
      if (withReconMs != null) {
        System.out.printf(
            Locale.ROOT,
            "spark reserialize overhead vs with_reconstruction   : %.2fx%n",
            reserializeMs / withReconMs);
      }
    }
    System.out.println();
    System.out.println("Per-column shred score:");
    int agg = 0;
    for (ColumnScore cs : scores) {
      System.out.printf(Locale.ROOT, "  %-20s score=%+d  %s%n", cs.fieldName, cs.score, cs.reason);
      agg += cs.score;
    }
    System.out.println(
        "  AGGREGATE            score=" + agg + "  (sum over " + scores.size() + " shredded cols)");
  }
}
