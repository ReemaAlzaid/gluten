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

import org.apache.spark.SparkConf

/**
 * Regression tests for GLUTEN-12471: broadcast hash joins on the cuDF (GPU) backend silently
 * returned empty results because CudfHashJoin built its hash table from the empty build-side
 * iterator instead of the prebuilt CPU table.
 *
 * These tests require GPU hardware and a cuDF-enabled build, so they are skipped unless the env var
 * GLUTEN_TEST_CUDF=1 is set. Run on a GPU host:
 *
 * GLUTEN_TEST_CUDF=1 mvn test -Pbackends-velox \
 * -DwildcardSuites=org.apache.gluten.execution.CudfBroadcastJoinSuite
 */
class CudfBroadcastJoinSuite extends VeloxWholeStageTransformerSuite {

  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  private def cudfEnabled: Boolean = sys.env.get("GLUTEN_TEST_CUDF").contains("1")

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.gluten.sql.columnar.cudf", "true")
      .set("spark.gluten.sql.columnar.backend.velox.cudf.allowCpuFallback", "false")
      // Force the broadcast path: the small side (orders filter) fits easily.
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "4g")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (cudfEnabled) {
      createTPCHNotNullTables()
    }
  }

  test("GLUTEN-12471: cuDF broadcast hash join returns non-empty, correct results") {
    assume(cudfEnabled, "Set GLUTEN_TEST_CUDF=1 on a GPU host to run cuDF suites")

    val query =
      """
        |SELECT l.l_orderkey, o.o_orderdate, l.l_extendedprice
        |FROM lineitem l
        |JOIN orders o ON l.l_orderkey = o.o_orderkey
        |WHERE o.o_orderdate < date '1995-01-01'
        |""".stripMargin

    // runQueryAndCompare executes on Gluten AND vanilla Spark and compares
    // results -- this is the core regression check: before the fix, the Gluten
    // side returned 0 rows and the comparison failed.
    runQueryAndCompare(query) {
      df =>
        val plan = df.queryExecution.executedPlan
        // The join must actually be offloaded as a broadcast hash join, not
        // demoted or fallen back.
        val bhj = collect(plan) { case j: BroadcastHashJoinExecTransformer => j }
        assert(bhj.nonEmpty, s"expected an offloaded broadcast hash join, got:\n$plan")
        assert(df.count() > 0, "broadcast join must not return empty results (GLUTEN-12471)")
    }
  }

  test("GLUTEN-12471: CudfVector host read does not crash in ColumnarToRow (q15-style)") {
    assume(cudfEnabled, "Set GLUTEN_TEST_CUDF=1 on a GPU host to run cuDF suites")

    // Aggregation over VARCHAR + numeric columns whose result crosses the
    // GPU->CPU ColumnarToRow boundary -- the path that previously segfaulted
    // on device-resident CudfVector children (VARCHAR offsets, q15).
    val query =
      """
        |SELECT l_suppkey, sum(l_extendedprice * (1 - l_discount)) AS total
        |FROM lineitem
        |WHERE l_shipdate >= date '1996-01-01'
        |  AND l_shipdate < date '1996-04-01'
        |GROUP BY l_suppkey
        |ORDER BY total DESC
        |LIMIT 10
        |""".stripMargin

    runQueryAndCompare(query) {
      df => assert(df.count() > 0, "aggregate over GPU batches must survive host reads")
    }
  }
}
