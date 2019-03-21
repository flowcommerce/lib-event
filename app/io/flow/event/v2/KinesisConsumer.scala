package io.flow.event.v2

import java.net.InetAddress
import java.util.UUID
import java.util.concurrent.Executors

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput}
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel
import io.flow.event.Record
import io.flow.log.RollbarLogger
import io.flow.util.FlowEnvironment
import org.joda.time.DateTime

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

case class KinesisConsumer (
  config: StreamConfig,
  f: Seq[Record] => Unit,
  logger: RollbarLogger
) extends StreamUsage {

  private[this] val workerId = Seq(
    config.appName,
    InetAddress.getLocalHost.getCanonicalHostName,
    UUID.randomUUID.toString
  ).mkString(":")

  private[this] val dynamoCapacity = {
    FlowEnvironment.Current match {
      case FlowEnvironment.Production => 10 // 10 is the default value in the AWS SDK
      case FlowEnvironment.Development | FlowEnvironment.Workstation => 1
    }
  }

  private[this] val worker = new Worker.Builder()
    .recordProcessorFactory(KinesisRecordProcessorFactory(config, workerId, f, logger))
    .config(
      new KinesisClientLibConfiguration(
        config.appName,
        config.streamName,
        config.awsCredentialsProvider,
        workerId
      ).withTableName(config.dynamoTableName)
        .withInitialLeaseTableReadCapacity(dynamoCapacity)
        .withInitialLeaseTableWriteCapacity(dynamoCapacity)
        .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
        .withCleanupLeasesUponShardCompletion(true)
        .withIdleTimeBetweenReadsInMillis(config.idleTimeBetweenReadsInMillis.toLong)
        .withMaxRecords(config.maxRecords)
        .withMetricsLevel(MetricsLevel.NONE)
        .withFailoverTimeMillis(30000) // See https://github.com/awslabs/amazon-kinesis-connectors/issues/10
    ).kinesisClient(config.kinesisClient)
    .build()

  private[this] val exec = Executors.newSingleThreadExecutor()

  logger
    .withKeyValue("class", this.getClass.getName)
    .withKeyValue("stream", config.streamName)
    .withKeyValue("worker_id", workerId)
    .info("Started")

  exec.execute(worker)

  def shutdown(): Unit = {
    exec.shutdown()
  }

}

case class KinesisRecordProcessorFactory(
  config: StreamConfig,
  workerId: String,
  f: Seq[Record] => Unit,
  logger: RollbarLogger
) extends IRecordProcessorFactory {

  override def createProcessor(): IRecordProcessor = {
    KinesisRecordProcessor(config, workerId, f, logger)
  }

}

case class KinesisRecordProcessor[T](
  config: StreamConfig,
  workerId: String,
  f: Seq[Record] => Unit,
  logger: RollbarLogger
) extends IRecordProcessor {

  import KinesisRecordProcessor._

  private val logger_ = logger
    .withKeyValue("class", this.getClass.getName)
    .withKeyValue("stream", config.streamName)
    .withKeyValue("worker_id", workerId)

  override def initialize(input: InitializationInput): Unit =
    logger_
      .withKeyValue("shard_id", input.getShardId)
      .info("Initializing")

  override def processRecords(input: ProcessRecordsInput): Unit = {
    logger_.info("Processing records")

    val kinesisRecords = input.getRecords.asScala
    val flowRecords = input.getRecords.asScala.map { record =>
      val buffer = record.getData
      val bytes = Array.fill[Byte](buffer.remaining)(0)
      buffer.get(bytes)

      Record.fromByteArray(
        arrivalTimestamp = new DateTime(record.getApproximateArrivalTimestamp),
        value = bytes
      )
    }

    executeRetry(flowRecords, 0)

    kinesisRecords.lastOption.foreach { record =>
      logger_.withKeyValue("checkpoint", record.getSequenceNumber).info("Checkpoint")
      input.getCheckpointer.checkpoint(record)
    }
  }

  @tailrec
  private def executeRetry(records: Seq[Record], retries: Int): Unit = {
    Try(f(records)) match {
      case Success(_) =>
      case Failure(NonFatal(e)) =>
        if (retries > MaxRetries) {
          logger_
            .withKeyValue("retries", retries)
            .error(s"[FlowKinesisError] Error while processing records after $MaxRetries", e)
          throw e
        } else {
          logger_
            .withKeyValue("retries", retries)
            .warn(s"[FlowKinesisWarn] Error while processing records (retry $retries/$MaxRetries). Retrying...", e)
          executeRetry(records, retries + 1)
        }
      case Failure(e) => throw e

    }
  }

  override def shutdown(input: ShutdownInput): Unit = {
    logger_.withKeyValue("reason", input.getShutdownReason.toString).info("Shutting down")
    try {
      input.getCheckpointer.checkpoint()
    } catch {
      // for instance lease has expired
      case NonFatal(e) =>
        logger_
          .withKeyValue("reason", input.getShutdownReason.toString)
          .error("[FlowKinesisError] Error while checkpointing when shutting down Kinesis consumer. Cannot checkpoint.", e)
    }
  }

}

object KinesisRecordProcessor {
  // Yes, it is arbitrary
  private val MaxRetries = 8
}
