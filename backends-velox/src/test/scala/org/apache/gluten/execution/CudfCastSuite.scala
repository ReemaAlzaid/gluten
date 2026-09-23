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
package org.apache.gluten.execution

import org.apache.gluten.tags.CudfTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.adaptive.ColumnarAQEShuffleReadExec

@CudfTest
class CudfCastSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.gluten.sql.columnar.cudf", "true")
      .set("spark.gluten.sql.columnar.backend.velox.cudf.allowCpuFallback", "false")
      .set("spark.sql.ansi.enabled", "false")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "4g")
  }

  test("cuDF legacy integer-to-double casts match Spark, including nested expressions") {
    withTempPath {
      dir =>
        withSQLConf(vanillaSparkConfs(): _*) {
          Seq(
            (Some(Int.MinValue), 0.5d),
            (Some(-1), 2.0d),
            (Some(0), 2.0d),
            (Some(1), 2.0d),
            (Some(16777217), 0.5d),
            (Some(Int.MaxValue), 0.5d),
            (None, 2.0d))
            .toDF("i", "factor")
            .write
            .parquet(dir.getCanonicalPath)
        }

        withTempView("legacy_cast_input") {
          spark.read.parquet(dir.getCanonicalPath).createOrReplaceTempView("legacy_cast_input")
          // MIN preserves INTEGER and keeps casts in the final aggregation stage
          // after the shuffle, where the GPU stage assertion applies.
          val query =
            """
              |SELECT i,
              |       CAST(min(i) AS DOUBLE) AS d,
              |       CAST(min(i) AS DOUBLE) * min(factor) AS product,
              |       CAST(min(i) AS DOUBLE) > min(factor) AS above
              |FROM legacy_cast_input
              |GROUP BY i
              |""".stripMargin

          runQueryAndCompare(query) {
            df =>
              val readers = getExecutedPlan(df).collect {
                case reader: ColumnarAQEShuffleReadExec => reader
              }
              assert(
                readers.exists(_.executionMode == MockGPUStageMode),
                s"expected a cuDF-tagged cast stage, got:\n${df.queryExecution.executedPlan}")
          }
        }
    }
  }
}
