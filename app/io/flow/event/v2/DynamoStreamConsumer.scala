package io.flow.event.v2

import com.amazonaws.auth.AWSCredentialsProviderChain
import com.amazonaws.services.dynamodbv2.streamsadapter.StreamsWorkerFactory
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.amazonaws.services.kinesis.clientlibrary.exceptions.{InvalidStateException, KinesisClientLibRetryableException, ShutdownException}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput}
import io.flow.event.{DynamoStreamRecord, Record}
import io.flow.log.RollbarLogger
import io.flow.play.metrics.MetricsSystem

import java.util.concurrent.{ExecutionException, TimeUnit, TimeoutException}
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

case class DynamoStreamConsumer(
  config: DynamoStreamConfig,
  creds: AWSCredentialsProviderChain,
  f: Seq[Record] => Unit,
  metrics: MetricsSystem,
  rollbarLogger: RollbarLogger,
) extends KinesisStyleConsumer(config, rollbarLogger) {

  private val worker = StreamsWorkerFactory.createDynamoDbStreamsWorker(
    new DynamoStreamRecordProcessorFactory(
      config = config,
      f = f,
      metrics = metrics,
      logger = logger
    ),
    config.toKclConfig(creds),
    config.kinesisClient,
    config.dynamoDBClient,
    config.cloudWatchClient
  )

  override def start(): Unit = {
    logger.info("Started")
    executor.execute(worker)
  }

  def shutdown(): Unit = {
    // kill the consumers first
    try {
      logger.info("Shutting down consumer")
      if (worker.startGracefulShutdown().get(2, TimeUnit.MINUTES))
        logger.info("Worker gracefully shutdown")
      else
        logger.warn("Worker terminated with exception")
    } catch {
      case e: TimeoutException =>
        logger.error("Worker termination timed out", e)
      case e @ (_: InterruptedException | _: ExecutionException) =>
        logger.error("Worker terminated with exception", e)
    }

    // then shut down the Executor and wait for all Runnables to finish
    executor.shutdown()
    if (executor.awaitTermination(2, TimeUnit.MINUTES))
      logger.info("Worker executor terminated")
    else
      logger.warn("Worker executor termination timed out")
  }
}

class DynamoStreamRecordProcessorFactory(
  config: DynamoStreamConfig,
  f: Seq[Record] => Unit,
  metrics: MetricsSystem,
  logger: RollbarLogger
) extends IRecordProcessorFactory {

  override def createProcessor(): IRecordProcessor =
    new DynamoStreamRecordProcessor(config, f, metrics, logger)
}

class DynamoStreamRecordProcessor(
  override val config: DynamoStreamConfig,
  f: Seq[Record] => Unit,
  override val metrics: MetricsSystem,
  override val rollbarLogger: RollbarLogger
) extends IRecordProcessor with KinesisStyleRecordProcessor {

  private val recordType = config.eventClass

  override def initialize(input: InitializationInput): Unit =
    logger
      .withKeyValue("shard_id", input.getShardId)
      .info("Initializing")

  override def shutdown(input: ShutdownInput): Unit = {
    logger.withKeyValue("shutdown_reason", input.getShutdownReason.toString).info("Shutting down")
    if (input.getShutdownReason == ShutdownReason.TERMINATE) {
      handleCheckpoint(input.getCheckpointer)
    }
  }

  override def processRecords(input: ProcessRecordsInput): Unit = {
    logger.withKeyValue("count", input.getRecords.size).info("Processing records")

    streamLagMetric.update(input.getMillisBehindLatest)
    numRecordsMetric.update(input.getRecords.size)

    val kinesisRecords = input.getRecords.asScala.toSeq
    val sequenceNumbers = kinesisRecords.map(_.getSequenceNumber)
    if (kinesisRecords.nonEmpty) {
      val flowRecords = kinesisRecords.flatMap { record =>
        record match {
          case adapter: RecordAdapter =>
            Option(adapter.getInternalObject).map(DynamoStreamRecord.apply(recordType, _))
          case _ =>
            logger.withKeyValue("record_type", record.getClass.getName).warn("Unknown record type")
            None
        }
      }
      executeRetry(f, flowRecords, sequenceNumbers)
    }

    kinesisRecords.lastOption.foreach { record =>
      logger.withKeyValue("checkpoint", record.getSequenceNumber).info("Checkpoint")
      handleCheckpoint(input.getCheckpointer)
    }
  }

  @tailrec
  protected final def handleCheckpoint(checkpointer: IRecordProcessorCheckpointer, retries: Int = 0): Unit = {

    import KinesisStyleRecordProcessor._
    try {
      checkpointer.checkpoint()
    }  catch {
      // Ignore handleCheckpoint if the processor instance has been shutdown (fail over).
      // i.e. Can't update handleCheckpoint - instance doesn't hold the lease for this shard.
      case e: ShutdownException =>
        logger.info("[FlowKinesisInfo] Caught error while checkpointing. Skipping checkpoint.", e)

      // Backoff and re-attempt handleCheckpoint upon transient failures
      // ThrottlingException | KinesisClientLibDependencyException
      case e: KinesisClientLibRetryableException =>
        if (retries >= MaxRetries) {
          logger.error(s"[FlowKinesisError] Error while checkpointing after $MaxRetries attempts", e)
        } else {
          logger.warn(s"[FlowKinesisWarn] Transient issue while checkpointing. Attempt ${retries + 1} of $MaxRetries.", e)
          Thread.sleep(BackoffTimeInMillis)
          handleCheckpoint(checkpointer, retries + 1)
        }

      // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
      case e: InvalidStateException =>
        logger.error("[FlowKinesisError] Error while checkpointing. Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e)
    }
  }
}