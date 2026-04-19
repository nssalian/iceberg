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
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;

/**
 * A {@link MetricsReporter} that accumulates all {@link ScanReport} and {@link CommitReport}
 * instances. Unlike {@link org.apache.iceberg.metrics.InMemoryMetricsReporter} which keeps only the
 * last report, this accumulates all reports for benchmark analysis.
 *
 * <p>Thread-safe: multiple concurrent writers and readers are supported.
 */
public class BenchmarkMetricsReporter implements MetricsReporter {

  private final List<ScanReport> scanReports =
      Collections.synchronizedList(new ArrayList<>());
  private final List<CommitReport> commitReports =
      Collections.synchronizedList(new ArrayList<>());

  @Override
  public void report(MetricsReport report) {
    if (report instanceof ScanReport scanReport) {
      scanReports.add(scanReport);
    } else if (report instanceof CommitReport commitReport) {
      commitReports.add(commitReport);
    }
  }

  public List<ScanReport> scanReports() {
    return Collections.unmodifiableList(new ArrayList<>(scanReports));
  }

  public List<CommitReport> commitReports() {
    return Collections.unmodifiableList(new ArrayList<>(commitReports));
  }

  public void clear() {
    scanReports.clear();
    commitReports.clear();
  }
}
