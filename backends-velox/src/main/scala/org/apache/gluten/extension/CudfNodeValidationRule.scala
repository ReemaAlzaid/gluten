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
package org.apache.gluten.extension

import org.apache.gluten.config.{GlutenConfig, VeloxConfig}
import org.apache.gluten.cudf.VeloxCudfPlanValidatorJniWrapper
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.execution._
import org.apache.gluten.extension.CudfNodeValidationRule.{createGPUColumnarExchange, setTagForWholeStageTransformer}

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, GPUColumnarShuffleExchangeExec, SparkPlan}

// Add the node name prefix 'Cudf' to GlutenPlan when can offload to cudf
case class CudfNodeValidationRule(glutenConf: GlutenConfig) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!glutenConf.enableColumnarCudf) {
      return plan
    }
    val transformedPlan = plan.transformUp {
      case shuffle @ ColumnarShuffleExchangeExec(
            _,
            VeloxResizeBatchesExec(w: WholeStageTransformer, _, _, _),
            _,
            _,
            _) =>
        setTagForWholeStageTransformer(w)
        createGPUColumnarExchange(shuffle)
      case shuffle @ ColumnarShuffleExchangeExec(_, w: WholeStageTransformer, _, _, _) =>
        setTagForWholeStageTransformer(w)
        createGPUColumnarExchange(shuffle)
      case transformer: WholeStageTransformer =>
        setTagForWholeStageTransformer(transformer)
        transformer
    }
    transformedPlan
  }
}

object CudfNodeValidationRule {
  def setTagForWholeStageTransformer(transformer: WholeStageTransformer): Unit = {
    // Spark 3.2 does not have TreeNode.exists, so use find(...).isDefined.
    val hasLeaf = transformer.find {
      case _: LeafTransformSupport => true
      case _ => false
    }.isDefined

    val canOffload = decideOffload(
      hasLeaf,
      VeloxConfig.get.cudfEnableTableScan,
      VeloxConfig.get.cudfEnableValidation,
      () =>
        VeloxCudfPlanValidatorJniWrapper.validate(
          transformer.substraitPlan.toProtobuf.toByteArray))

    if (canOffload) {
      transformer.foreach {
        case _: LeafTransformSupport =>
        case t: TransformSupport =>
          t.setTagValue(CudfTag.CudfTag, true)
        case _ =>
      }
      transformer.setTagValue(CudfTag.CudfTag, true)
    } else {
      transformer.setTagValue(CudfTag.CudfTag, false)
    }
  }

  /**
   * Decide whether a whole-stage transformer can be offloaded to the cuDF GPU backend.
   *
   * Pure (no native calls) so the branching can be unit-tested:
   *   - a stage that reads a table is offloadable only when GPU table scan is enabled;
   *   - otherwise, when validation is enabled, the native validator decides (it exempts
   *     TableScan, so a scan-bearing stage passes only when every other operator is
   *     cuDF-capable);
   *   - when validation is disabled, the stage is offloaded optimistically.
   *
   * `validate` is invoked only on the validation path, never when a table-reading stage is
   * rejected up front, so the previous "tag GPU unconditionally when table scan is enabled"
   * behavior no longer skips validation.
   */
  private[extension] def decideOffload(
      hasLeaf: Boolean,
      enableTableScan: Boolean,
      enableValidation: Boolean,
      validate: () => Boolean): Boolean = {
    if (hasLeaf && !enableTableScan) {
      false
    } else if (!enableValidation) {
      true
    } else {
      validate()
    }
  }

  def createGPUColumnarExchange(shuffle: ColumnarShuffleExchangeExec): SparkPlan = {
    val exec = GPUColumnarShuffleExchangeExec(
      shuffle.outputPartitioning,
      shuffle.child,
      shuffle.shuffleOrigin,
      shuffle.projectOutputAttributes,
      shuffle.advisoryPartitionSize)
    val res = exec.doValidate()
    if (!res.ok()) {
      throw new GlutenNotSupportException(res.reason())
    }
    exec
  }
}
