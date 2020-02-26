package io.flow.event.v2

import java.util.concurrent.{ExecutionException, Executors, TimeUnit, TimeoutException}

import com.amazonaws.auth.AWSCredentialsProviderChain
import com.amazonaws.services.kinesis.clientlibrary.exceptions._
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{ShutdownReason, Worker}
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput}
import io.flow.event.Record
import io.flow.log.RollbarLogger
import io.flow.play.metrics.MetricsSystem
import org.joda.time.DateTime

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

case class KinesisConsumer (
  config: StreamConfig,
  creds: AWSCredentialsProviderChain,
  f: Seq[Record] => Unit,
  metrics: MetricsSystem,
  logger: RollbarLogger,
) extends StreamUsage {

  private[this] val worker = new Worker.Builder()
    .recordProcessorFactory(KinesisRecordProcessorFactory(config, f, metrics, logger))
    .config(config.toKclConfig(creds))
    .kinesisClient(config.kinesisClient)
    .build()

  private[this] val exec = Executors.newSingleThreadExecutor()

  private[this] val logger_ =
    logger
      .withKeyValue("class", this.getClass.getName)
      .withKeyValue("stream", config.streamName)
      .withKeyValue("worker_id", config.workerId)

  logger_.info("Started")
  exec.execute(worker)

  def shutdown(): Unit = {
    // kill the consumers first
    try {
      logger_.info("Shutting down consumer")
      if (worker.startGracefulShutdown().get(2, TimeUnit.MINUTES))
        logger_.info("Worker gracefully shutdown")
      else
        logger_.warn("Worker terminated with exception")
    } catch {
      case _: TimeoutException =>
        logger_.error("Worker termination timed out")
      case e @ (_: InterruptedException | _: ExecutionException) =>
        logger_.error("Worker terminated with exception", e)
    }

    // then shut down the Executor and wait for all Runnables to finish
    exec.shutdown()
    if (exec.awaitTermination(2, TimeUnit.MINUTES))
      logger_.info("Worker executor terminated")
    else
      logger_.warn("Worker executor termination timed out")

    ()
  }

}

case class KinesisRecordProcessorFactory(
  config: StreamConfig,
  f: Seq[Record] => Unit,
  metrics: MetricsSystem,
  logger: RollbarLogger,
) extends IRecordProcessorFactory {

  override def createProcessor(): IRecordProcessor = {
    KinesisRecordProcessor(config, f, metrics, logger)
  }

}

object KinesisRecordProcessor {
  // Yes, it is arbitrary
  private val MaxRetries = 8
  private val BackoffTimeInMillis = 3000L
}

case class KinesisRecordProcessor[T](
  config: StreamConfig,
  f: Seq[Record] => Unit,
  metrics: MetricsSystem,
  logger: RollbarLogger,
) extends IRecordProcessor {

  import KinesisRecordProcessor._

  val streamLagMetric = metrics.registry.histogram(s"${config.streamName}.consumer.lagMillis")
  val numRecordsMetric = metrics.registry.histogram(s"${config.streamName}.consumer.numRecords")

  private val logger_ = logger
    .withKeyValue("class", this.getClass.getName)
    .withKeyValue("stream", config.streamName)
    .withKeyValue("worker_id", config.workerId)

  override def initialize(input: InitializationInput): Unit =
    logger_
      .withKeyValue("shard_id", input.getShardId)
      .info("Initializing")

  override def processRecords(input: ProcessRecordsInput): Unit = {
    logger_.withKeyValue("count", input.getRecords.size).info("Processing records")

    streamLagMetric.update(input.getMillisBehindLatest)
    numRecordsMetric.update(input.getRecords.size)

    val kinesisRecords = input.getRecords.asScala

    if (kinesisRecords.nonEmpty) {
      val flowRecords = kinesisRecords.map { record =>
        val buffer = record.getData
        val bytes = Array.fill[Byte](buffer.remaining)(0)
        buffer.get(bytes)

        Record.fromByteArray(
          arrivalTimestamp = new DateTime(record.getApproximateArrivalTimestamp),
          value = bytes
        )
      }
      val sequenceNumbers = kinesisRecords.map(_.getSequenceNumber)
      executeRetry(flowRecords.toSeq, sequenceNumbers.toSeq)
    }

    kinesisRecords.lastOption.foreach { record =>
      logger_.withKeyValue("checkpoint", record.getSequenceNumber).info("Checkpoint")
      handleCheckpoint(input.getCheckpointer)
    }
  }

  @tailrec
  private def executeRetry(records: Seq[Record], sequenceNumbers: Seq[String], retries: Int = 0): Unit = {
    Try(f(records)) match {
      case Success(_) =>
      case Failure(NonFatal(e)) =>
        if (retries >= MaxRetries) {
          val size = records.size
          logger_
            .withKeyValue("retries", retries)
            .error(s"[FlowKinesisError] Error while processing records after $MaxRetries attempts. " +
              s"$size records are skipped. Sequence numbers: ${sequenceNumbers.mkString(", ")}", e)
        } else {
          logger_
            .withKeyValue("retries", retries)
            .warn(s"[FlowKinesisWarn] Error while processing records (retry $retries/$MaxRetries). Retrying...", e)
          Thread.sleep(BackoffTimeInMillis)
          executeRetry(records, sequenceNumbers, retries + 1)
        }
      case Failure(e) => throw e
    }
  }

  override def shutdown(input: ShutdownInput): Unit = {
    logger_.withKeyValue("reason", input.getShutdownReason.toString).info("Shutting down")
    if (input.getShutdownReason == ShutdownReason.TERMINATE) {
      handleCheckpoint(input.getCheckpointer)
    }
  }

  private def handleCheckpoint(checkpointer: IRecordProcessorCheckpointer, retries: Int = 0): Unit = {

    import KinesisRecordProcessor._
    try {
      checkpointer.checkpoint()
    }  catch {
      // Ignore handleCheckpoint if the processor instance has been shutdown (fail over).
      // i.e. Can't update handleCheckpoint - instance doesn't hold the lease for this shard.
      case e: ShutdownException =>
        logger_.info("[FlowKinesisInfo] Caught error while checkpointing. Skipping checkpoint.", e)

      // Backoff and re-attempt handleCheckpoint upon transient failures
      // ThrottlingException | KinesisClientLibDependencyException
      case e: KinesisClientLibRetryableException =>
        if (retries >= MaxRetries) {
          logger_.error(s"[FlowKinesisError] Error while checkpointing after $MaxRetries attempts", e)
        } else {
          logger_.warn(s"[FlowKinesisWarn] Transient issue while checkpointing. Attempt ${retries + 1} of $MaxRetries.", e)
          Thread.sleep(BackoffTimeInMillis)
          handleCheckpoint(checkpointer, retries + 1)
        }

      // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
      case e: InvalidStateException =>
        logger_.error("[FlowKinesisError] Error while checkpointing. Cannot save handleCheckpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e)
    }
  }

}
