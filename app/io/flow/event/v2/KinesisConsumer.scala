package io.flow.event.v2

import java.net.InetAddress
import java.util.UUID
import java.util.concurrent.Executors

import com.amazonaws.services.kinesis.clientlibrary.exceptions._
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, ShutdownReason, Worker}
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

  protected[v2] val kclConfig = {
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
      .withIdleTimeBetweenReadsInMillis(config.idleTimeBetweenReadsInMillis.fold(KinesisClientLibConfiguration.DEFAULT_IDLETIME_BETWEEN_READS_MILLIS)(_.toLong))
      .withMaxRecords(config.maxRecords.getOrElse(1000))
      .withMetricsLevel(MetricsLevel.NONE)
      .withFailoverTimeMillis(30000) // See https://github.com/awslabs/amazon-kinesis-connectors/issues/10
  }

  private[this] val worker = new Worker.Builder()
    .recordProcessorFactory(KinesisRecordProcessorFactory(config, workerId, f, logger))
    .config(kclConfig)
    .kinesisClient(config.kinesisClient)
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

object KinesisRecordProcessor {
  // Yes, it is arbitrary
  private val MaxRetries = 8
  private val BackoffTimeInMillis = 3000L
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
      executeRetry(flowRecords, sequenceNumbers)
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
