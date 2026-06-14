/*
 * Native Substrait -> Spark DataFrame translator (bounded).
 *
 * Why: io.substrait:spark's ToLogicalPlan mis-builds the FileScan for LocalFiles reads,
 * so a filter/aggregate over a Parquet file fails task-result deserialization
 * ("unread block data"). A NATIVE spark.read.parquet(...).filter(...).groupBy(...) over
 * the SAME data runs fine through this server AND offloads to Gluten/Velox-cuDF (GPU).
 *
 * So for file-backed plans we translate the Substrait relation tree directly into
 * native DataFrame operations, bypassing substrait-spark entirely. Returns None when it
 * meets a node/expression it doesn't handle, so the caller falls back to substrait-spark
 * (which already works for inline/virtual-table data).
 *
 * Scope: ReadRel(LocalFiles parquet | NamedTable) -> Filter -> Project -> Aggregate ->
 * Fetch(limit) -> Sort, with field refs, literals, common comparison/arithmetic/boolean
 * scalar functions, and count/sum/min/max/avg aggregates. Expand as needed.
 */
package org.apache.gluten.substrait.gpu

import scala.collection.JavaConverters._
import scala.util.Try

import io.substrait.proto.{AggregateRel, Expression, Plan => ProtoPlan, ReadRel, Rel}

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.{functions => F}

object SubstraitNativeTranslator {

  /** Try to fully translate a Substrait plan to a native DataFrame. None => caller falls back. */
  def tryTranslate(plan: ProtoPlan, spark: SparkSession): Option[DataFrame] = {
    Try {
      val fnMap = functionMap(plan)
      val root = plan.getRelations(0)
      val rel = if (root.hasRoot) root.getRoot.getInput else root.getRel
      translateRel(rel, spark, fnMap)
    }.toOption
  }

  /** anchor -> function name (e.g. 1 -> "gt:any_any"). */
  private def functionMap(plan: ProtoPlan): Map[Int, String] =
    plan.getExtensionsList.asScala.collect {
      case e if e.hasExtensionFunction =>
        e.getExtensionFunction.getFunctionAnchor -> e.getExtensionFunction.getName
    }.toMap

  private def shortName(name: String): String = name.split(":")(0)

  private def translateRel(rel: Rel, spark: SparkSession, fn: Map[Int, String]): DataFrame = {
    import io.substrait.proto.Rel.RelTypeCase._
    rel.getRelTypeCase match {
      case READ => translateRead(rel.getRead, spark)

      case FILTER =>
        val child = translateRel(rel.getFilter.getInput, spark, fn)
        child.filter(translateExpr(rel.getFilter.getCondition, child, fn))

      case PROJECT =>
        val child = translateRel(rel.getProject.getInput, spark, fn)
        val cols = rel.getProject.getExpressionsList.asScala.zipWithIndex.map {
          case (e, i) => translateExpr(e, child, fn).as(s"col$i")
        }
        // Substrait Project emits ONLY the new expressions (the emit usually drops inputs).
        child.select(cols.toSeq: _*)

      case AGGREGATE => translateAggregate(rel.getAggregate, spark, fn)

      case FETCH =>
        val child = translateRel(rel.getFetch.getInput, spark, fn)
        val n = rel.getFetch.getCount.toInt
        if (n >= 0) child.limit(n) else child

      case SORT =>
        val child = translateRel(rel.getSort.getInput, spark, fn)
        val orderings = rel.getSort.getSortsList.asScala.map { sf =>
          val c = translateExpr(sf.getExpr, child, fn)
          // SORT_DIRECTION_ASC_* = 1/3 ; DESC = 2/4
          if (sf.getDirectionValue == 2 || sf.getDirectionValue == 4) c.desc else c.asc
        }
        child.orderBy(orderings.toSeq: _*)

      case _ => throw new UnsupportedOperationException(s"native: rel ${rel.getRelTypeCase}")
    }
  }

  private def translateRead(read: ReadRel, spark: SparkSession): DataFrame = {
    if (read.hasNamedTable) {
      spark.table(read.getNamedTable.getNamesList.asScala.mkString("."))
    } else if (read.hasLocalFiles) {
      val paths = read.getLocalFiles.getItemsList.asScala.map(_.getUriFile).filter(_.nonEmpty)
      // formats: parquet is the common case; orc supported by Spark too.
      val isOrc = read.getLocalFiles.getItemsList.asScala.headOption.exists(_.hasOrc)
      if (isOrc) spark.read.orc(paths.toSeq: _*) else spark.read.parquet(paths.toSeq: _*)
    } else {
      throw new UnsupportedOperationException("native: non-file read (virtual table)")
    }
  }

  private def translateAggregate(agg: AggregateRel, spark: SparkSession, fn: Map[Int, String]): DataFrame = {
    val child = translateRel(agg.getInput, spark, fn)
    val groupCols = agg.getGroupingsList.asScala.flatMap(_.getGroupingExpressionsList.asScala)
      .map(e => translateExpr(e, child, fn))
    val measureCols = agg.getMeasuresList.asScala.zipWithIndex.map { case (m, i) =>
      val af = m.getMeasure
      val name = shortName(fn.getOrElse(af.getFunctionReference,
        throw new UnsupportedOperationException("native: unknown agg fn ref")))
      val args = af.getArgumentsList.asScala.map(a => translateExpr(a.getValue, child, fn))
      val col = name match {
        case "count" => if (args.isEmpty) F.count(F.lit(1)) else F.count(args.head)
        case "sum" => F.sum(args.head)
        case "min" => F.min(args.head)
        case "max" => F.max(args.head)
        case "avg" | "mean" => F.avg(args.head)
        case other => throw new UnsupportedOperationException(s"native: agg fn $other")
      }
      col.as(s"agg$i")
    }
    if (groupCols.isEmpty) child.agg(measureCols.head, measureCols.tail.toSeq: _*)
    else child.groupBy(groupCols.toSeq: _*).agg(measureCols.head, measureCols.tail.toSeq: _*)
  }

  private def translateExpr(e: Expression, input: DataFrame, fn: Map[Int, String]): Column = {
    import io.substrait.proto.Expression.RexTypeCase._
    e.getRexTypeCase match {
      case SELECTION =>
        val idx = e.getSelection.getDirectReference.getStructField.getField
        F.col(input.columns(idx))

      case LITERAL => translateLiteral(e.getLiteral)

      case SCALAR_FUNCTION =>
        val sf = e.getScalarFunction
        val name = shortName(fn.getOrElse(sf.getFunctionReference,
          throw new UnsupportedOperationException("native: unknown scalar fn ref")))
        val a = sf.getArgumentsList.asScala.map(arg => translateExpr(arg.getValue, input, fn))
        applyScalar(name, a.toSeq)

      case _ => throw new UnsupportedOperationException(s"native: expr ${e.getRexTypeCase}")
    }
  }

  private def applyScalar(name: String, a: Seq[Column]): Column = name match {
    case "gt" => a(0) > a(1)
    case "lt" => a(0) < a(1)
    case "gte" => a(0) >= a(1)
    case "lte" => a(0) <= a(1)
    case "equal" => a(0) === a(1)
    case "not_equal" => a(0) =!= a(1)
    case "and" => a.reduce(_ && _)
    case "or" => a.reduce(_ || _)
    case "not" => !a(0)
    case "add" => a(0) + a(1)
    case "subtract" => a(0) - a(1)
    case "multiply" => a(0) * a(1)
    case "divide" => a(0) / a(1)
    case "is_null" => a(0).isNull
    case "is_not_null" => a(0).isNotNull
    case other => throw new UnsupportedOperationException(s"native: scalar fn $other")
  }

  private def translateLiteral(lit: Expression.Literal): Column = {
    import io.substrait.proto.Expression.Literal.LiteralTypeCase._
    lit.getLiteralTypeCase match {
      case BOOLEAN => F.lit(lit.getBoolean)
      case I8 => F.lit(lit.getI8)
      case I16 => F.lit(lit.getI16)
      case I32 => F.lit(lit.getI32)
      case I64 => F.lit(lit.getI64)
      case FP32 => F.lit(lit.getFp32)
      case FP64 => F.lit(lit.getFp64)
      case STRING => F.lit(lit.getString)
      case other => throw new UnsupportedOperationException(s"native: literal $other")
    }
  }
}
