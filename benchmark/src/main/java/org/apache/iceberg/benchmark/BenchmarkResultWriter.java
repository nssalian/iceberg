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
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.CommitReportParser;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.ScanReportParser;

/** Writes benchmark results (timing, scan reports, commit reports) to JSON files. */
public class BenchmarkResultWriter {

  private BenchmarkResultWriter() {}

  /**
   * Write timing results as a JSON object with per-iteration entries and a summary block that
   * exposes min/p25/p50/p75/max so downstream scoring can compute IQR without re-reading the
   * iteration list.
   */
  public static void writeTimings(Path outputDir, String operation, List<Long> timingsNs)
      throws IOException {
    Path file = outputDir.resolve("timing.json");
    StringBuilder sb = new StringBuilder();
    sb.append("{\n  \"operation\": \"").append(operation).append("\",\n");
    sb.append("  \"iterations\": [\n");
    for (int i = 0; i < timingsNs.size(); i++) {
      sb.append("    {\"iteration\": ")
          .append(i)
          .append(", \"wall_clock_ns\": ")
          .append(timingsNs.get(i))
          .append(", \"wall_clock_ms\": ")
          .append(timingsNs.get(i) / 1_000_000)
          .append("}");
      if (i < timingsNs.size() - 1) {
        sb.append(",");
      }
      sb.append("\n");
    }
    sb.append("  ],\n  \"summary\": ");
    sb.append(timingsSummaryJson(timingsNs));
    sb.append("\n}\n");
    writeString(file, sb.toString());
  }

  private static String timingsSummaryJson(List<Long> timingsNs) {
    if (timingsNs.isEmpty()) {
      return "{}";
    }
    long[] arr = new long[timingsNs.size()];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = timingsNs.get(i);
    }
    long[] sorted = arr.clone();
    java.util.Arrays.sort(sorted);
    long min = sorted[0];
    long max = sorted[sorted.length - 1];
    long p25 = percentile(arr, 25);
    long p50 = percentile(arr, 50);
    long p75 = percentile(arr, 75);
    return String.format(
        java.util.Locale.ROOT,
        "{\"min_ns\": %d, \"p25_ns\": %d, \"p50_ns\": %d, \"p75_ns\": %d, \"max_ns\": %d, "
            + "\"min_ms\": %d, \"p25_ms\": %d, \"p50_ms\": %d, \"p75_ms\": %d, \"max_ms\": %d}",
        min,
        p25,
        p50,
        p75,
        max,
        min / 1_000_000,
        p25 / 1_000_000,
        p50 / 1_000_000,
        p75 / 1_000_000,
        max / 1_000_000);
  }

  /** Write all accumulated scan reports as a JSON array. */
  public static void writeScanReports(Path outputDir, List<ScanReport> reports) throws IOException {
    Path file = outputDir.resolve("scan-reports.json");
    StringBuilder sb = new StringBuilder();
    sb.append("[\n");
    for (int i = 0; i < reports.size(); i++) {
      sb.append("  ").append(ScanReportParser.toJson(reports.get(i)));
      if (i < reports.size() - 1) {
        sb.append(",");
      }
      sb.append("\n");
    }
    sb.append("]\n");
    writeString(file, sb.toString());
  }

  /** Write all accumulated commit reports as a JSON array. */
  public static void writeCommitReports(Path outputDir, List<CommitReport> reports)
      throws IOException {
    Path file = outputDir.resolve("commit-reports.json");
    StringBuilder sb = new StringBuilder();
    sb.append("[\n");
    for (int i = 0; i < reports.size(); i++) {
      sb.append("  ").append(CommitReportParser.toJson(reports.get(i)));
      if (i < reports.size() - 1) {
        sb.append(",");
      }
      sb.append("\n");
    }
    sb.append("]\n");
    writeString(file, sb.toString());
  }

  private static void writeString(Path file, String content) throws IOException {
    Files.createDirectories(file.getParent());
    try (Writer writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
      writer.write(content);
    }
  }

  /**
   * Write Spark engine metrics per iteration plus an aggregated summary. Format:
   *
   * <pre>
   * {
   *   "operation": "write-shredded",
   *   "iterations": [
   *     {"iteration": 0, "executor_cpu_time_ns": ..., "executor_run_time_ms": ..., ...},
   *     ...
   *   ],
   *   "summary": {"executor_cpu_time_ns_total": ..., "p50_executor_run_time_ms": ..., ...}
   * }
   * </pre>
   *
   * <p>Both per-iteration and aggregated views are kept so the scorer can either roll up or surface
   * iteration variance without re-reading the file.
   */
  public static void writeSparkMetrics(
      Path outputDir, String operation, List<SparkMetricsListener.SparkMetricsSummary> perIter)
      throws IOException {
    Path file = outputDir.resolve("spark-metrics.json");
    StringBuilder sb = new StringBuilder();
    sb.append("{\n  \"operation\": \"").append(operation).append("\",\n");
    sb.append("  \"iterations\": [\n");
    for (int i = 0; i < perIter.size(); i++) {
      SparkMetricsListener.SparkMetricsSummary m = perIter.get(i);
      sb.append("    {\"iteration\": ")
          .append(i)
          .append(", \"executor_cpu_time_ns\": ")
          .append(m.executorCpuTimeNs)
          .append(", \"executor_run_time_ms\": ")
          .append(m.executorRunTimeMs)
          .append(", \"jvm_gc_time_ms\": ")
          .append(m.jvmGcTimeMs)
          .append(", \"records_read\": ")
          .append(m.recordsRead)
          .append(", \"bytes_read\": ")
          .append(m.bytesRead)
          .append(", \"records_written\": ")
          .append(m.recordsWritten)
          .append(", \"bytes_written\": ")
          .append(m.bytesWritten)
          .append(", \"shuffle_bytes_read\": ")
          .append(m.shuffleBytesRead)
          .append(", \"shuffle_bytes_written\": ")
          .append(m.shuffleBytesWritten)
          .append(", \"peak_execution_memory_bytes\": ")
          .append(m.peakExecutionMemoryBytes)
          .append(", \"result_serialization_time_ms\": ")
          .append(m.resultSerializationTimeMs)
          .append("}");
      if (i < perIter.size() - 1) {
        sb.append(",");
      }
      sb.append("\n");
    }
    sb.append("  ],\n  \"summary\": ");
    sb.append(summaryJson(perIter));
    sb.append("\n}\n");
    writeString(file, sb.toString());
  }

  private static String summaryJson(List<SparkMetricsListener.SparkMetricsSummary> perIter) {
    if (perIter.isEmpty()) {
      return "{}";
    }
    long cpuTotal = 0;
    long runTotal = 0;
    long gcTotal = 0;
    long recordsReadTotal = 0;
    long bytesReadTotal = 0;
    long recordsWrittenTotal = 0;
    long bytesWrittenTotal = 0;
    long shuffleReadTotal = 0;
    long shuffleWriteTotal = 0;
    long peakMemMax = 0;
    long[] runMs = new long[perIter.size()];
    long[] cpuMs = new long[perIter.size()];
    long[] gcMs = new long[perIter.size()];
    for (int i = 0; i < perIter.size(); i++) {
      SparkMetricsListener.SparkMetricsSummary m = perIter.get(i);
      cpuTotal += m.executorCpuTimeNs;
      runTotal += m.executorRunTimeMs;
      gcTotal += m.jvmGcTimeMs;
      recordsReadTotal += m.recordsRead;
      bytesReadTotal += m.bytesRead;
      recordsWrittenTotal += m.recordsWritten;
      bytesWrittenTotal += m.bytesWritten;
      shuffleReadTotal += m.shuffleBytesRead;
      shuffleWriteTotal += m.shuffleBytesWritten;
      peakMemMax = Math.max(peakMemMax, m.peakExecutionMemoryBytes);
      runMs[i] = m.executorRunTimeMs;
      cpuMs[i] = m.executorCpuTimeNs / 1_000_000L;
      gcMs[i] = m.jvmGcTimeMs;
    }
    return String.format(
        java.util.Locale.ROOT,
        """
        {
            "executor_cpu_time_ns_total": %d,
            "executor_run_time_ms_total": %d,
            "jvm_gc_time_ms_total": %d,
            "records_read_total": %d,
            "bytes_read_total": %d,
            "records_written_total": %d,
            "bytes_written_total": %d,
            "shuffle_bytes_read_total": %d,
            "shuffle_bytes_written_total": %d,
            "peak_execution_memory_bytes_max": %d,
            "p50_executor_run_time_ms": %d,
            "p95_executor_run_time_ms": %d,
            "p50_executor_cpu_time_ms": %d,
            "p50_jvm_gc_time_ms": %d
          }""",
        cpuTotal,
        runTotal,
        gcTotal,
        recordsReadTotal,
        bytesReadTotal,
        recordsWrittenTotal,
        bytesWrittenTotal,
        shuffleReadTotal,
        shuffleWriteTotal,
        peakMemMax,
        percentile(runMs, 50),
        percentile(runMs, 95),
        percentile(cpuMs, 50),
        percentile(gcMs, 50));
  }

  private static long percentile(long[] values, int pct) {
    if (values.length == 0) {
      return 0;
    }
    long[] sorted = values.clone();
    java.util.Arrays.sort(sorted);
    int idx = Math.min(sorted.length - 1, (int) Math.floor((sorted.length - 1) * pct / 100.0));
    return sorted[idx];
  }

  /** Write file size summary for a write operation: total bytes + file count. */
  public static void writeFileSize(Path outputDir, String operation, long totalBytes, int fileCount)
      throws IOException {
    // Per-op file so multiple write ops in the same cell (write-variant + write-arrays-variant
    // + write-arrays-shredded, etc.) do not clobber each other.
    Path file = outputDir.resolve("file-size-" + operation + ".json");
    String json =
        String.format(
            java.util.Locale.ROOT,
            "{\n  \"operation\": \"%s\",\n  \"file_bytes\": %d,\n  \"file_count\": %d\n}\n",
            operation,
            totalBytes,
            fileCount);
    writeString(file, json);
    // Compat alias: write-variant and write-shredded are the canonical events writes. Mirror them
    // to the legacy "file-size.json" path so older scoring code keeps working. write-arrays-* and
    // other ops only land in their per-op file.
    if ("write-variant".equals(operation) || "write-shredded".equals(operation)) {
      writeString(outputDir.resolve("file-size.json"), json);
    }
  }

  /**
   * Write a per-op correctness map. Each entry captures (row_count, sum_value) from a verification
   * query. The scorer compares shredded cells against the unshredded baseline cell for the same
   * workload to compute RPR (row preservation) and AFD (aggregate fidelity).
   */
  public static void writeCorrectness(Path outputDir, java.util.Map<String, long[]> perOp)
      throws IOException {
    Path file = outputDir.resolve("correctness.json");
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    int i = 0;
    int n = perOp.size();
    for (java.util.Map.Entry<String, long[]> entry : perOp.entrySet()) {
      long[] values = entry.getValue();
      sb.append("  \"")
          .append(entry.getKey())
          .append("\": {\"row_count\": ")
          .append(values[0])
          .append(", \"sum_value\": ")
          .append(values[1])
          .append("}");
      if (i < n - 1) {
        sb.append(",");
      }
      sb.append("\n");
      i++;
    }
    sb.append("}\n");
    writeString(file, sb.toString());
  }
}
