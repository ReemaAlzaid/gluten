/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.benchmark

import org.apache.spark.SparkFunSuite

class KafkaBenchmarkResultSuite extends SparkFunSuite {
  test("count sink rows when native source metrics report zero") {
    val samples = Seq(KafkaBenchmarkSample(0, 0, 4), KafkaBenchmarkSample(1, 0, 6))
    assert(KafkaBenchmarkResult.validatedRowCount(samples, 10) == 10)
  }

  test("accept completed batches with no output rows") {
    val samples = Seq(KafkaBenchmarkSample(0, 4, 4), KafkaBenchmarkSample(1, 0, 0))
    assert(KafkaBenchmarkResult.validatedRowCount(samples, 4) == 4)
  }

  test("reject unavailable or negative sink row counts") {
    Seq(-1L, -2L).foreach {
      sinkRows =>
        intercept[IllegalArgumentException] {
          KafkaBenchmarkResult.validatedRowCount(
            Seq(
              KafkaBenchmarkSample(0, 0, sinkRows),
              KafkaBenchmarkSample(1, 0, 10 - sinkRows)),
            10)
        }
    }
  }

  test("reject truncated, missing, duplicated, or reordered batch progress") {
    val invalidBatchIds = Seq(Seq(1L, 2L), Seq(0L, 2L), Seq(0L, 0L), Seq(1L, 0L))
    invalidBatchIds.foreach {
      batchIds =>
        val samples = batchIds.map(KafkaBenchmarkSample(_, 0, 5))
        intercept[IllegalArgumentException] {
          KafkaBenchmarkResult.validatedRowCount(samples, 10)
        }
    }
  }

  test("reject too few or too many sink rows") {
    Seq(9L, 11L).foreach {
      sinkRows =>
        intercept[IllegalArgumentException] {
          KafkaBenchmarkResult.validatedRowCount(
            Seq(KafkaBenchmarkSample(0, 10, sinkRows)),
            10)
        }
    }
  }

  test("reject empty progress") {
    intercept[IllegalArgumentException] {
      KafkaBenchmarkResult.validatedRowCount(Seq.empty, 10)
    }
  }

  test("reject nonpositive expected row counts") {
    Seq(0L, -1L).foreach {
      expectedRows =>
        intercept[IllegalArgumentException] {
          KafkaBenchmarkResult.validatedRowCount(
            Seq(KafkaBenchmarkSample(0, 1, 1)),
            expectedRows)
        }
    }
  }

  test("accept resumed batch progress without Parquet sink row metrics") {
    val samples = (4L to 6L).map(KafkaBenchmarkSample(_, 0, -1))
    KafkaBenchmarkResult.validateBatchSequence(samples, 4, 6)
  }

  test("reject missing initial, final, or intermediate resumed batches") {
    val invalidBatchIds = Seq(
      Seq(5L, 6L),
      Seq(4L, 5L),
      Seq(4L, 6L),
      Seq(4L, 5L, 5L, 6L),
      Seq(4L, 6L, 5L),
      Seq(4L, 5L, 6L, 7L))
    invalidBatchIds.foreach {
      batchIds =>
        val samples = batchIds.map(KafkaBenchmarkSample(_, 0, -1))
        intercept[IllegalArgumentException] {
          KafkaBenchmarkResult.validateBatchSequence(samples, 4, 6)
        }
    }
  }

  test("reject empty resumed batch progress") {
    intercept[IllegalArgumentException] {
      KafkaBenchmarkResult.validateBatchSequence(Seq.empty, 4, 6)
    }
  }

  test("reject negative or reversed expected batch endpoints") {
    intercept[IllegalArgumentException] {
      KafkaBenchmarkResult.validateBatchSequence(Seq(KafkaBenchmarkSample(-1, 0, -1)), -1, -1)
    }
    intercept[IllegalArgumentException] {
      KafkaBenchmarkResult.validateBatchSequence(Seq(KafkaBenchmarkSample(4, 0, -1)), 4, 3)
    }
  }

  test("accept exactly the expected native or Spark scan") {
    KafkaBenchmarkResult.validateScanCounts(1, 0, nativeExpected = true)
    KafkaBenchmarkResult.validateScanCounts(0, 1, nativeExpected = false)
  }

  test("reject scan fallback, missing scans, or multiple scans") {
    val invalidCounts = Seq(
      (0, 1, true),
      (1, 0, false),
      (0, 0, true),
      (0, 0, false),
      (2, 0, true),
      (0, 2, false),
      (1, 1, true),
      (1, 1, false))
    invalidCounts.foreach {
      case (nativeScans, sparkScans, nativeExpected) =>
        intercept[IllegalArgumentException] {
          KafkaBenchmarkResult.validateScanCounts(nativeScans, sparkScans, nativeExpected)
        }
    }
  }
}
