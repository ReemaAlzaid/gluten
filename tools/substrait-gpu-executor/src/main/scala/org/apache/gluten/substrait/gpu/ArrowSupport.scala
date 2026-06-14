/*
 * Spark <-> Arrow bridging for the Flight SQL server.
 *
 * MVP: covers the common scalar types. Nested/complex types (array, map, struct)
 * are intentionally TODO and will throw a clear error so unsupported plans fail
 * loudly rather than silently mis-encoding. Results are materialized with
 * collect() for simplicity; switch to df.toLocalIterator + batching for large
 * results.
 */
package org.apache.gluten.substrait.gpu

import scala.collection.JavaConverters._

import org.apache.arrow.flight.FlightProducer.ServerStreamListener
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object ArrowSupport {

  private val BATCH_SIZE = 10000

  /** Map a Spark StructType to an Arrow Schema. */
  def toArrowSchema(schema: StructType): Schema = {
    val fields = schema.fields.map(f => toArrowField(f.name, f.dataType, f.nullable))
    new Schema(fields.toList.asJava)
  }

  private def toArrowField(name: String, dt: DataType, nullable: Boolean): Field = {
    val arrowType: ArrowType = dt match {
      case BooleanType => ArrowType.Bool.INSTANCE
      case ByteType => new ArrowType.Int(8, true)
      case ShortType => new ArrowType.Int(16, true)
      case IntegerType => new ArrowType.Int(32, true)
      case LongType => new ArrowType.Int(64, true)
      case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
      case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      case StringType => ArrowType.Utf8.INSTANCE
      case BinaryType => ArrowType.Binary.INSTANCE
      case DateType => new ArrowType.Date(DateUnit.DAY)
      case TimestampType => new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")
      case d: DecimalType => new ArrowType.Decimal(d.precision, d.scale, 128)
      case other =>
        throw new UnsupportedOperationException(
          s"Type not yet supported by the Gluten Substrait Flight SQL server: $other")
    }
    new Field(name, new FieldType(nullable, arrowType, null), java.util.Collections.emptyList())
  }

  /** Stream a DataFrame to the Flight listener as one or more Arrow batches. */
  def stream(df: DataFrame, allocator: BufferAllocator, listener: ServerStreamListener): Unit = {
    val schema = toArrowSchema(df.schema)
    val root = VectorSchemaRoot.create(schema, allocator)
    try {
      listener.start(root)
      val sparkTypes = df.schema.fields.map(_.dataType)
      // NOTE: collect() (one job, driver thread already set up by the caller) rather than
      // toLocalIterator() (per-partition jobs) — the latter fails task-result deserialization
      // ("unread block data") when driven from a Flight gRPC handler thread.
      val it = df.collect().iterator
      var rowsInBatch = 0
      root.allocateNew()
      while (it.hasNext) {
        val row = it.next()
        var c = 0
        while (c < sparkTypes.length) {
          setValue(root.getVector(c), rowsInBatch, sparkTypes(c), row, c)
          c += 1
        }
        rowsInBatch += 1
        if (rowsInBatch == BATCH_SIZE) {
          root.setRowCount(rowsInBatch)
          listener.putNext()
          root.clear()
          root.allocateNew()
          rowsInBatch = 0
        }
      }
      if (rowsInBatch > 0) {
        root.setRowCount(rowsInBatch)
        listener.putNext()
      }
      listener.completed()
    } finally {
      root.close()
    }
  }

  private def setValue(
      vector: FieldVector, idx: Int, dt: DataType, row: org.apache.spark.sql.Row, col: Int): Unit = {
    if (row.isNullAt(col)) {
      vector.setNull(idx)
      return
    }
    dt match {
      case BooleanType => vector.asInstanceOf[BitVector].setSafe(idx, if (row.getBoolean(col)) 1 else 0)
      case ByteType => vector.asInstanceOf[TinyIntVector].setSafe(idx, row.getByte(col))
      case ShortType => vector.asInstanceOf[SmallIntVector].setSafe(idx, row.getShort(col))
      case IntegerType => vector.asInstanceOf[IntVector].setSafe(idx, row.getInt(col))
      case LongType => vector.asInstanceOf[BigIntVector].setSafe(idx, row.getLong(col))
      case FloatType => vector.asInstanceOf[Float4Vector].setSafe(idx, row.getFloat(col))
      case DoubleType => vector.asInstanceOf[Float8Vector].setSafe(idx, row.getDouble(col))
      case StringType =>
        vector.asInstanceOf[VarCharVector]
          .setSafe(idx, row.getString(col).getBytes(java.nio.charset.StandardCharsets.UTF_8))
      case BinaryType =>
        vector.asInstanceOf[VarBinaryVector].setSafe(idx, row.getAs[Array[Byte]](col))
      case DateType =>
        val days = row.getDate(col).toLocalDate.toEpochDay.toInt
        vector.asInstanceOf[DateDayVector].setSafe(idx, days)
      case TimestampType =>
        val inst = row.getTimestamp(col).toInstant
        val micros = inst.getEpochSecond * 1000000L + inst.getNano / 1000L
        vector.asInstanceOf[TimeStampMicroTZVector].setSafe(idx, micros)
      case _: DecimalType =>
        vector.asInstanceOf[DecimalVector].setSafe(idx, row.getDecimal(col))
      case other =>
        throw new UnsupportedOperationException(s"Type not yet supported: $other")
    }
  }
}
