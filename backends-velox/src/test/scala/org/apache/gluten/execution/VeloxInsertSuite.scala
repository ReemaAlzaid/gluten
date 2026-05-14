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
import org.apache.spark.sql.Row
import org.apache.spark.sql.internal.SQLConf

class VeloxInsertSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "placeholder"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
  }

  test("storeAssignmentPolicy default ANSI is independent from ANSI mode") {
    withTable("store_assignment_ansi") {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        assert(SQLConf.get.storeAssignmentPolicy == SQLConf.StoreAssignmentPolicy.ANSI)

        spark.sql("CREATE TABLE store_assignment_ansi (c INT) USING PARQUET")
        val exception = intercept[Exception] {
          spark.sql("INSERT INTO store_assignment_ansi SELECT '2147483648'").collect()
        }
        val message = exceptionMessages(exception)
        assert(message.contains("2147483648"), message)
        assert(message.contains("CAST_INVALID_INPUT") || message.contains("Cannot cast"), message)

        withSQLConf(
          SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.LEGACY.toString) {
          spark.sql("INSERT INTO store_assignment_ansi SELECT '2147483648'").collect()
          checkAnswer(spark.table("store_assignment_ansi"), Row(null))
        }
      }
    }
  }

  private def exceptionMessages(e: Throwable): String = {
    val message = Option(e.getMessage).getOrElse("")
    if (e.getCause == null) {
      message
    } else {
      message + "\n" + exceptionMessages(e.getCause)
    }
  }
}
