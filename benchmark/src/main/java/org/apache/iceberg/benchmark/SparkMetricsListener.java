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

import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

/**
 * Accumulates per-task Spark engine metrics for one benchmark iteration. Driver calls {@link
 * #snapshot()} after each iteration to capture totals, then {@link #reset()} before the next
 * iteration. Engine metrics complement wall-clock timing by exposing where time is spent (CPU vs GC
 * vs shuffle) and how much data was actually read/written, which is essential for an engine
 * comparison benchmark.
 */
public class SparkMetricsListener extends SparkListener {
  private long executorCpuTimeNs;
  private long executorRunTimeMs;
  private long jvmGcTimeMs;
  private long recordsRead;
  private long bytesRead;
  private long recordsWritten;
  private long bytesWritten;
  private long shuffleBytesRead;
  private long shuffleBytesWritten;
  private long peakExecutionMemoryBytes;
  private long resultSerializationTimeMs;

  @Override
  public synchronized void onTaskEnd(SparkListenerTaskEnd event) {
    TaskMetrics tm = event.taskMetrics();
    if (tm == null) {
      return;
    }
    executorCpuTimeNs += tm.executorCpuTime();
    executorRunTimeMs += tm.executorRunTime();
    jvmGcTimeMs += tm.jvmGCTime();
    recordsRead += tm.inputMetrics().recordsRead();
    bytesRead += tm.inputMetrics().bytesRead();
    recordsWritten += tm.outputMetrics().recordsWritten();
    bytesWritten += tm.outputMetrics().bytesWritten();
    shuffleBytesRead += tm.shuffleReadMetrics().totalBytesRead();
    shuffleBytesWritten += tm.shuffleWriteMetrics().bytesWritten();
    peakExecutionMemoryBytes = Math.max(peakExecutionMemoryBytes, tm.peakExecutionMemory());
    resultSerializationTimeMs += tm.resultSerializationTime();
  }

  /** Snapshot the current accumulated counters as an immutable summary. */
  public synchronized SparkMetricsSummary snapshot() {
    return new SparkMetricsSummary(
        executorCpuTimeNs,
        executorRunTimeMs,
        jvmGcTimeMs,
        recordsRead,
        bytesRead,
        recordsWritten,
        bytesWritten,
        shuffleBytesRead,
        shuffleBytesWritten,
        peakExecutionMemoryBytes,
        resultSerializationTimeMs);
  }

  /** Reset all counters to zero. Call between iterations. */
  public synchronized void reset() {
    executorCpuTimeNs = 0;
    executorRunTimeMs = 0;
    jvmGcTimeMs = 0;
    recordsRead = 0;
    bytesRead = 0;
    recordsWritten = 0;
    bytesWritten = 0;
    shuffleBytesRead = 0;
    shuffleBytesWritten = 0;
    peakExecutionMemoryBytes = 0;
    resultSerializationTimeMs = 0;
  }

  /** Immutable snapshot of Spark task metrics, units in field names. */
  public static final class SparkMetricsSummary {
    public final long executorCpuTimeNs;
    public final long executorRunTimeMs;
    public final long jvmGcTimeMs;
    public final long recordsRead;
    public final long bytesRead;
    public final long recordsWritten;
    public final long bytesWritten;
    public final long shuffleBytesRead;
    public final long shuffleBytesWritten;
    public final long peakExecutionMemoryBytes;
    public final long resultSerializationTimeMs;

    SparkMetricsSummary(
        long executorCpuTimeNs,
        long executorRunTimeMs,
        long jvmGcTimeMs,
        long recordsRead,
        long bytesRead,
        long recordsWritten,
        long bytesWritten,
        long shuffleBytesRead,
        long shuffleBytesWritten,
        long peakExecutionMemoryBytes,
        long resultSerializationTimeMs) {
      this.executorCpuTimeNs = executorCpuTimeNs;
      this.executorRunTimeMs = executorRunTimeMs;
      this.jvmGcTimeMs = jvmGcTimeMs;
      this.recordsRead = recordsRead;
      this.bytesRead = bytesRead;
      this.recordsWritten = recordsWritten;
      this.bytesWritten = bytesWritten;
      this.shuffleBytesRead = shuffleBytesRead;
      this.shuffleBytesWritten = shuffleBytesWritten;
      this.peakExecutionMemoryBytes = peakExecutionMemoryBytes;
      this.resultSerializationTimeMs = resultSerializationTimeMs;
    }
  }
}
