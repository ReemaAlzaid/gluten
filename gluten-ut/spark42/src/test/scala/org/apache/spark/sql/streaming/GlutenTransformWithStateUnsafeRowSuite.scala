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
package org.apache.spark.sql.streaming

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.GlutenStreamingSQLTestsTrait
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{RocksDBStateStoreProvider, StateStoreValueSchemaNotCompatible}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock

class GlutenTransformWithStateUnsafeRowSuite
  extends TransformWithStateUnsafeRowSuite
  with GlutenStreamingSQLTestsTrait {

  import testImplicits._

  Seq(false, true).foreach {
    replayLastBatch =>
      testGluten(s"introducing TTL after restart fails query (replayLastBatch=$replayLastBatch)") {
        withSQLConf(
          SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName,
          SQLConf.SHUFFLE_PARTITIONS.key ->
            TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString
        ) {
          withTempDir {
            checkpointDir =>
              val inputData = MemoryStream[String]
              val groupedInput = inputData.toDS().groupByKey(x => x)
              val clock = new StreamManualClock
              val startStream = StartStream(
                trigger = Trigger.ProcessingTime("1 second"),
                checkpointLocation = checkpointDir.getCanonicalPath,
                triggerClock = clock)
              val result = groupedInput.transformWithState(
                new RunningCountStatefulProcessor(),
                TimeMode.ProcessingTime(),
                OutputMode.Update())

              testStream(result, OutputMode.Update())(
                startStream,
                AddData(inputData, "a"),
                AdvanceManualClock(1 * 1000),
                CheckNewAnswer(("a", "1")),
                AddData(inputData, "b"),
                AdvanceManualClock(1 * 1000),
                CheckNewAnswer(("b", "1")),
                StopStream,
                Execute {
                  query =>
                    if (replayLastBatch) {
                      // Simulate interruption after the latest batch's offsets and state were written
                      // but before its commit marker became durable. Spark must replay it on restart.
                      val lastCommittedBatch = query.commitLog.getLatest().get._1
                      assert(lastCommittedBatch >= 1)
                      assert(query.commitLog.get(lastCommittedBatch - 1).nonEmpty)
                      assert(query.offsetLog.getLatest().get._1 == lastCommittedBatch)
                      query.commitLog.purgeAfter(lastCommittedBatch - 1)
                      assert(query.commitLog.getLatest().get._1 == lastCommittedBatch - 1)
                    }
                }
              )

              val result2 = groupedInput.transformWithState(
                new RunningCountStatefulProcessorWithTTL(),
                TimeMode.ProcessingTime(),
                OutputMode.Update())

              // GLUTEN-13463: The restarted query may reject the schema before AddData can synchronize
              // with the manual clock. Queue data before starting and check the failure immediately.
              testStream(result2, OutputMode.Update())(
                AddData(inputData, "a"),
                startStream,
                ExpectFailure[StateStoreValueSchemaNotCompatible] {
                  t =>
                    checkError(
                      t.asInstanceOf[SparkUnsupportedOperationException],
                      condition = "STATE_STORE_VALUE_SCHEMA_NOT_COMPATIBLE",
                      parameters = Map(
                        "storedValueSchema" -> "StructType(StructField(value,LongType,false))",
                        "newValueSchema" ->
                          ("StructType(StructField(value,StructType(StructField(value,LongType,false))," +
                            "true),StructField(ttlExpirationMs,LongType,true))")
                      )
                    )
                }
              )
          }
        }
      }
  }
}
