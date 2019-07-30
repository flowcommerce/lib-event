package io.flow.event.v2

import java.util.concurrent.{Executors, TimeUnit}

import io.flow.event.Record
import io.flow.log.RollbarLogger
import io.flow.play.metrics.MetricsSystem
import org.joda.time.DateTime
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.exceptions.{InvalidStateException, KinesisClientLibRetryableException, ShutdownException}
import software.amazon.kinesis.lifecycle.events.{InitializationInput, LeaseLostInput, ProcessRecordsInput, ShardEndedInput, ShutdownRequestedInput}
import software.amazon.kinesis.processor.{RecordProcessorCheckpointer, ShardRecordProcessor, ShardRecordProcessorFactory}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

case class KinesisConsumer (
  config: StreamConfig,
  creds: AwsCredentialsProviderChain,
  f: Seq[Record] => Unit,
  metrics: MetricsSystem,
  logger: RollbarLogger,
) extends StreamUsage {

  private[this] val scheduler = {
    val recordProcessorFactory = KinesisRecordProcessorFactory(config, f, metrics, logger)

    val consumerConfig = ConsumerConfig(config, creds, recordProcessorFactory)

    new Scheduler(
      consumerConfig.checkpointConfig,
      consumerConfig.coordinatorConfig,
      consumerConfig.leaseManagementConfig,
      consumerConfig.lifecycleConfig,
      consumerConfig.metricsConfig,
      consumerConfig.processorConfig,
      consumerConfig.retrievalConfig
    )
  }

  private[this] val exec = Executors.newSingleThreadExecutor()

  private[this] val logger_ =
    logger
      .withKeyValue("class", this.getClass.getName)
      .withKeyValue("stream", config.streamName)
      .withKeyValue("worker_id", config.workerId)

  logger_.info("Started")
  exec.execute(scheduler)

  def shutdown(): Unit = {
    Try {
      logger_.info("Shutting down consumers")
      scheduler.startGracefulShutdown().get(1, TimeUnit.MINUTES)
    } match {
      case Success(_) => ()
      case Failure(_) => scheduler.shutdown()
    }

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
) extends ShardRecordProcessorFactory {

  override def shardRecordProcessor(): ShardRecordProcessor = {
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
) extends ShardRecordProcessor {

  import KinesisRecordProcessor._

  val streamLagMetric = metrics.registry.histogram(s"${config.streamName}.consumer.lagMillis")
  val numRecordsMetric = metrics.registry.histogram(s"${config.streamName}.consumer.numRecords")

  private val logger_ = logger
    .withKeyValue("class", this.getClass.getName)
    .withKeyValue("stream", config.streamName)
    .withKeyValue("worker_id", config.workerId)

  override def initialize(input: InitializationInput): Unit =
    logger_
      .withKeyValue("shard_id", input.shardId)
      .info("Initializing")

  override def processRecords(input: ProcessRecordsInput): Unit = {
    logger_.withKeyValue("count", input.records.size).info("Processing records")

    streamLagMetric.update(input.millisBehindLatest)
    numRecordsMetric.update(input.records.size)

    val kinesisRecords = input.records.asScala

    if (kinesisRecords.nonEmpty) {
      val flowRecords = kinesisRecords.map { record =>
        val buffer = record.data
        val bytes = Array.fill[Byte](buffer.remaining)(0)
        buffer.get(bytes)

        Record.fromByteArray(
          arrivalTimestamp = new DateTime(record.approximateArrivalTimestamp.toEpochMilli),
          value = bytes
        )
      }
      val sequenceNumbers = kinesisRecords.map(_.sequenceNumber)
      executeRetry(flowRecords, sequenceNumbers)
    }

    kinesisRecords.lastOption.foreach { record =>
      logger_.withKeyValue("checkpoint", record.sequenceNumber).info("Checkpoint")
      handleCheckpoint(input.checkpointer)
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

  override def shutdownRequested(input: ShutdownRequestedInput): Unit = {
    logger_.withKeyValue("reason", "requested").info("Shutting down")
    handleCheckpoint(input.checkpointer, inShutdown = true)
  }

  override def leaseLost(input: LeaseLostInput): Unit = {
    logger_.withKeyValue("reason", "leaseLost").info("Shutting down")
  }

  override def shardEnded(input: ShardEndedInput): Unit = {
    logger_.withKeyValue("reason", "shardEnded").info("Shutting down")
    handleCheckpoint(input.checkpointer, inShutdown = true)
  }

  private def handleCheckpoint(checkpointer: RecordProcessorCheckpointer, retries: Int = 0, inShutdown: Boolean = false): Unit = {

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
          val msg = s"[FlowKinesisError] Error while checkpointing after $MaxRetries attempts"
          if (inShutdown) {
            logger_.info(msg, e)
          } else {
            logger_.error(msg, e)
          }
        } else {
          val msg = s"[FlowKinesisWarn] Transient issue while checkpointing. Attempt ${retries + 1} of $MaxRetries."
          if (inShutdown) {
            logger_.info(msg, e)
          } else {
            logger_.warn(msg, e)
          }
          Thread.sleep(BackoffTimeInMillis)
          handleCheckpoint(checkpointer, retries + 1)
        }

      // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
      case e: InvalidStateException =>
        val msg = "[FlowKinesisError] Error while checkpointing. Cannot save handleCheckpoint to the DynamoDB table used by the Amazon Kinesis Client Library."
        if (inShutdown) {
          logger_.info(msg, e)
        } else {
          logger_.error(msg, e)
        }
    }
  }
}
