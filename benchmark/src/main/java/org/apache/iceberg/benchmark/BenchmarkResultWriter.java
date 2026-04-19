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

  /** Write timing results as a JSON array of objects with iteration and wall_clock_ns fields. */
  public static void writeTimings(Path outputDir, String operation, List<Long> timingsNs)
      throws IOException {
    Path file = outputDir.resolve("timing.json");
    StringBuilder sb = new StringBuilder();
    sb.append("[\n");
    for (int i = 0; i < timingsNs.size(); i++) {
      sb.append("  {\"operation\": \"")
          .append(operation)
          .append("\", \"iteration\": ")
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
    sb.append("]\n");
    writeString(file, sb.toString());
  }

  /** Write all accumulated scan reports as a JSON array. */
  public static void writeScanReports(Path outputDir, List<ScanReport> reports)
      throws IOException {
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
}
