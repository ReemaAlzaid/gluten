/*
 * Lives in org.apache.spark.sql purely to reach the package-private
 * Dataset.ofRows, which turns a Catalyst LogicalPlan into a DataFrame.
 */
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object GlutenLogicalPlanBridge {
  def ofRows(spark: SparkSession, plan: LogicalPlan): DataFrame =
    Dataset.ofRows(spark, plan)
}
