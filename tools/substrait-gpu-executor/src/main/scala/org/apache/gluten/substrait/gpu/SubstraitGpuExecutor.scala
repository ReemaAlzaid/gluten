/*
 * Gluten Substrait GPU Executor
 *
 * Accepts a Substrait plan over Arrow Flight SQL, converts it to a Spark
 * LogicalPlan (io.substrait:spark), runs it on a Gluten-enabled SparkSession,
 * and streams the result back as Arrow.
 *
 * Wiring:  PySpark --SparkConnect--> spark-substrait-gateway
 *                  --Substrait/FlightSQL--> THIS SERVER --> Spark + Gluten/Velox
 *
 * Gluten is enabled purely through the spark-submit --conf flags used to launch
 * this server (spark.plugins=org.apache.gluten.GlutenPlugin, offHeap, columnar
 * shuffle, etc.). The server code itself is backend-agnostic: with no Gluten
 * jar/conf it runs on vanilla Spark, which is how we test stages 1-2.
 */
package org.apache.gluten.substrait.gpu

import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import com.google.protobuf.{Any => ProtoAny, ByteString}

import org.apache.arrow.flight.{FlightDescriptor, FlightEndpoint, FlightInfo, FlightProducer, FlightServer, Location, Ticket}
import org.apache.arrow.flight.FlightProducer.{CallContext, ServerStreamListener}
import org.apache.arrow.flight.sql.NoOpFlightSqlProducer
import org.apache.arrow.flight.sql.impl.FlightSql.{CommandStatementSubstraitPlan, TicketStatementQuery}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

import org.apache.spark.sql.{DataFrame, GlutenLogicalPlanBridge, SparkSession}

import io.substrait.plan.ProtoPlanConverter
import io.substrait.proto.{Plan => ProtoPlan, ReadRel, Rel}
import io.substrait.spark.logical.ToLogicalPlan

/**
 * Flight SQL producer that executes incoming Substrait plans on Spark+Gluten.
 *
 * Flow:
 *  - getFlightInfoSubstraitPlan: decode plan -> Spark LogicalPlan -> DataFrame,
 *    stash the DataFrame under a handle, return a FlightInfo whose ticket carries
 *    that handle (as a TicketStatementQuery).
 *  - getStreamStatement: look the DataFrame up by handle and stream it as Arrow.
 */
class GlutenSubstraitProducer(
    spark: SparkSession,
    allocator: BufferAllocator,
    appClassLoader: ClassLoader)
    extends NoOpFlightSqlProducer {

  // handle -> already-planned DataFrame, awaiting its getStream call.
  private val pending = new ConcurrentHashMap[String, DataFrame]()
  private val counter = new java.util.concurrent.atomic.AtomicLong(0L)

  // Flight runs handlers on gRPC worker threads whose context classloader is NOT
  // Spark's app/MutableURLClassLoader. Spark task (de)serialization keys off the
  // thread context classloader, so without this a real Spark job (e.g. a parquet
  // FileScan) fails to deserialize its task result with "unread block data".
  private def withSparkClassLoader[T](body: => T): T = {
    val prev = Thread.currentThread().getContextClassLoader
    Thread.currentThread().setContextClassLoader(appClassLoader)
    // The active/default SparkSession is thread-local; gRPC handler threads don't have
    // it set, which breaks Spark job execution (task-result deserialization). Set it.
    SparkSession.setActiveSession(spark)
    SparkSession.setDefaultSession(spark)
    try body
    finally Thread.currentThread().setContextClassLoader(prev)
  }

  override def getFlightInfoSubstraitPlan(
      command: CommandStatementSubstraitPlan,
      context: CallContext,
      descriptor: FlightDescriptor): FlightInfo = {
    val substraitBytes = command.getPlan.getPlan.toByteArray
    val df =
      try {
        withSparkClassLoader(substraitToDataFrame(substraitBytes))
      } catch {
        case t: Throwable =>
          // scalastyle:off println
          System.err.println("[flightsql] getFlightInfoSubstraitPlan failed:")
          t.printStackTrace()
          // scalastyle:on println
          throw t
      }

    val handle = "stmt-" + counter.incrementAndGet()
    pending.put(handle, df)

    val ticketCmd = TicketStatementQuery.newBuilder()
      .setStatementHandle(ByteString.copyFrom(handle, StandardCharsets.UTF_8))
      .build()
    val ticket = new Ticket(ProtoAny.pack(ticketCmd).toByteArray)

    val arrowSchema = ArrowSupport.toArrowSchema(df.schema)
    val endpoint = new FlightEndpoint(ticket)
    new FlightInfo(arrowSchema, descriptor, java.util.Collections.singletonList(endpoint), -1L, -1L)
  }

  override def getStreamStatement(
      ticket: TicketStatementQuery,
      context: CallContext,
      listener: ServerStreamListener): Unit = {
    val handle = ticket.getStatementHandle.toStringUtf8
    val df = pending.remove(handle)
    if (df == null) {
      listener.error(new IllegalStateException(s"Unknown statement handle: $handle"))
      return
    }
    try {
      withSparkClassLoader(ArrowSupport.stream(df, allocator, listener))
    } catch {
      case t: Throwable =>
        // scalastyle:off println
        System.err.println("[flightsql] getStreamStatement failed:")
        t.printStackTrace()
        // scalastyle:on println
        listener.error(t)
    }
  }

  /** Substrait protobuf bytes -> Spark DataFrame. */
  private def substraitToDataFrame(bytes: Array[Byte]): DataFrame = {
    val protoPlan = ProtoPlan.parseFrom(bytes)
    // scalastyle:off println
    // Prefer the native translator for file-backed plans: substrait-spark mis-builds the
    // FileScan for LocalFiles, so a filter/aggregate over Parquet fails ("unread block data").
    // Native spark.read.parquet(...) runs fine here AND offloads to Gluten/Velox-cuDF.
    // Fall back to substrait-spark for inline/virtual-table plans (which it handles).
    val df = SubstraitNativeTranslator.tryTranslate(protoPlan, spark) match {
      case Some(native) =>
        System.err.println("[flightsql] using NATIVE Substrait->DataFrame translation")
        native
      case None =>
        System.err.println("[flightsql] falling back to substrait-spark ToLogicalPlan")
        val plan = new ProtoPlanConverter().from(rewriteLocalFileReads(protoPlan))
        val logical = plan.getRoots.get(0).getInput.accept(new ToLogicalPlan(spark))
        GlutenLogicalPlanBridge.ofRows(spark, logical)
    }
    // Print the EXECUTED physical plan: Cudf* = GPU, Velox*/Transformer = CPU native, else fallback.
    System.err.println("[flightsql] ===== EXECUTED PLAN =====\n" +
      df.queryExecution.executedPlan.toString + "\n[flightsql] =========================")
    // scalastyle:on println
    df
  }

  // ---- LocalFiles -> native parquet temp view rewrite --------------------
  // substrait-spark's LocalFiles consumer builds a FileScan whose task results don't
  // deserialize. For each LocalFiles read we register the parquet path(s) as a native
  // Spark temp view and rewrite the read to a NamedTable referencing that view.

  private def rewriteLocalFileReads(plan: ProtoPlan): ProtoPlan = {
    val b = plan.toBuilder
    var i = 0
    while (i < b.getRelationsCount) {
      val pr = b.getRelations(i)
      if (pr.hasRoot) {
        val newRoot = pr.getRoot.toBuilder.setInput(rewriteRel(pr.getRoot.getInput)).build()
        b.setRelations(i, pr.toBuilder.setRoot(newRoot).build())
      } else if (pr.hasRel) {
        b.setRelations(i, pr.toBuilder.setRel(rewriteRel(pr.getRel)).build())
      }
      i += 1
    }
    b.build()
  }

  private def rewriteRel(rel: Rel): Rel = {
    import io.substrait.proto.Rel.RelTypeCase._
    rel.getRelTypeCase match {
      case READ => rel.toBuilder.setRead(rewriteRead(rel.getRead)).build()
      case FILTER =>
        val r = rel.getFilter; rel.toBuilder.setFilter(r.toBuilder.setInput(rewriteRel(r.getInput))).build()
      case PROJECT =>
        val r = rel.getProject; rel.toBuilder.setProject(r.toBuilder.setInput(rewriteRel(r.getInput))).build()
      case AGGREGATE =>
        val r = rel.getAggregate; rel.toBuilder.setAggregate(r.toBuilder.setInput(rewriteRel(r.getInput))).build()
      case FETCH =>
        val r = rel.getFetch; rel.toBuilder.setFetch(r.toBuilder.setInput(rewriteRel(r.getInput))).build()
      case SORT =>
        val r = rel.getSort; rel.toBuilder.setSort(r.toBuilder.setInput(rewriteRel(r.getInput))).build()
      case JOIN =>
        val r = rel.getJoin
        rel.toBuilder.setJoin(
          r.toBuilder.setLeft(rewriteRel(r.getLeft)).setRight(rewriteRel(r.getRight))).build()
      case _ => rel
    }
  }

  private def rewriteRead(read: ReadRel): ReadRel = {
    if (!read.hasLocalFiles) return read
    val paths = read.getLocalFiles.getItemsList.asScala.map(_.getUriFile).filter(_.nonEmpty).toArray
    if (paths.isEmpty) return read
    val viewName = "gluten_lf_" + counter.incrementAndGet()
    // NATIVE parquet read -> temp view (session-scoped; persists across the gRPC calls).
    spark.read.parquet(paths.toSeq: _*).createOrReplaceTempView(viewName)
    read.toBuilder
      .clearLocalFiles()
      .setNamedTable(ReadRel.NamedTable.newBuilder().addNames(viewName).build())
      .build()
  }
}

object SubstraitGpuExecutor {
  def main(args: Array[String]): Unit = {
    val host = sys.props.getOrElse("flightsql.host", "0.0.0.0")
    val port = sys.props.getOrElse("flightsql.port", "50052").toInt

    // master/configs (incl. all Gluten confs) come from spark-submit.
    val spark = SparkSession.builder()
      .appName("GlutenSubstraitGpuExecutor")
      .getOrCreate()

    val allocator = new RootAllocator(Long.MaxValue)
    // Capture the app/MutableURLClassLoader on the main thread (the one Spark uses
    // for task (de)serialization) so gRPC handler threads can adopt it.
    val appClassLoader = Thread.currentThread().getContextClassLoader
    val producer = new GlutenSubstraitProducer(spark, allocator, appClassLoader)
    val location = Location.forGrpcInsecure(host, port)
    val server = FlightServer.builder(allocator, location, producer).build()
    server.start()

    // scalastyle:off println
    println(s"Gluten Substrait Flight SQL server listening on grpc://$host:$port")
    println(s"Gluten plugin active: ${spark.conf.getOption("spark.plugins").exists(_.contains("Gluten"))}")
    // scalastyle:on println

    server.awaitTermination()
  }
}
