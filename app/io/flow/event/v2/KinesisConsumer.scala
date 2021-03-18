package io.flow.event.v2

import io.flow.event.Record
import io.flow.log.RollbarLogger
import io.flow.play.metrics.MetricsSystem
import io.flow.util.FlowEnvironment
import org.joda.time.DateTime
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.kinesis.common.{ConfigsBuilder, InitialPositionInStream, InitialPositionInStreamExtended}
import software.amazon.kinesis.coordinator.Scheduler
import software.amazon.kinesis.exceptions.{InvalidStateException, KinesisClientLibRetryableException, ShutdownException}
import software.amazon.kinesis.lifecycle.events._
import software.amazon.kinesis.processor.{RecordProcessorCheckpointer, ShardRecordProcessor, ShardRecordProcessorFactory}
import software.amazon.kinesis.retrieval.polling.{PollingConfig, SimpleRecordsFetcherFactory}

import java.net.URI
import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

case class KinesisConsumer(
  config: KinesisStreamConfig,
  creds: AwsCredentialsProviderChain,
  f: Seq[Record] => Unit,
  metrics: MetricsSystem,
  rollbarLogger: RollbarLogger,
) extends KinesisStyleConsumer(config, rollbarLogger) {

  private[this] val scheduler = {
    val recordProcessorFactory = KinesisRecordProcessorFactory(config, f, metrics, logger)

    val consumerConfig = new ConsumerConfig(config, creds, recordProcessorFactory)

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

  override def start(): Unit = {
    logger.info("Started")
    executor.execute(scheduler)
  }

  override def shutdown(): Unit = {
    Try {
      logger.info("Shutting down consumers")
      scheduler.startGracefulShutdown().get(1, TimeUnit.MINUTES)
    } match {
      case Success(_) => ()
      case Failure(_) => scheduler.shutdown()
    }

    executor.shutdown()
    if (executor.awaitTermination(2, TimeUnit.MINUTES))
      logger.info("Worker executor terminated")
    else
      logger.warn("Worker executor termination timed out")

    ()
  }

}


class ConsumerConfig(
  config: KinesisStreamConfig,
  creds: AwsCredentialsProviderChain,
  recordProcessorFactory: ShardRecordProcessorFactory
) {
  private val configsBuilder = {
    val dynamoBuilder = DynamoDbAsyncClient.builder.credentialsProvider(creds)
    for {
      ep <- config.endpoints.dynamodb
    } yield dynamoBuilder.endpointOverride(URI.create(ep))
    val dynamoClient = dynamoBuilder.build

    val cloudWatchClient = CloudWatchAsyncClient.builder.credentialsProvider(creds).build

    new ConfigsBuilder(
      config.streamName,
      config.appName,
      config.kinesisClient,
      dynamoClient,
      cloudWatchClient,
      config.workerId,
      recordProcessorFactory
    )
      .tableName(config.dynamoTableName)
  }

  private val dynamoCapacity = {
    FlowEnvironment.Current match {
      case FlowEnvironment.Production => 10 // 10 is the default value in the AWS SDK
      case FlowEnvironment.Development | FlowEnvironment.Workstation => 1
    }
  }

  val checkpointConfig = configsBuilder.checkpointConfig()

  val coordinatorConfig = configsBuilder.coordinatorConfig()
    .shardConsumerDispatchPollIntervalMillis(
      config.idleTimeBetweenReadsInMillis
        .getOrElse(configsBuilder.coordinatorConfig.shardConsumerDispatchPollIntervalMillis)
    )

  val leaseManagementConfig = configsBuilder.leaseManagementConfig()
    .initialLeaseTableReadCapacity(dynamoCapacity)
    .initialLeaseTableWriteCapacity(dynamoCapacity)
    .cleanupLeasesUponShardCompletion(true)
    .maxLeasesForWorker(config.maxLeasesForWorker.getOrElse(configsBuilder.leaseManagementConfig.maxLeasesForWorker))
    .maxLeasesToStealAtOneTime(config.maxLeasesToStealAtOneTime.getOrElse(configsBuilder.leaseManagementConfig.maxLeasesToStealAtOneTime))
    .failoverTimeMillis(30000) // See https://github.com/awslabs/amazon-kinesis-connectors/issues/10

  val lifecycleConfig = configsBuilder.lifecycleConfig()

  val metricsConfig = configsBuilder.metricsConfig()
    .metricsLevel(config.metricsLevel)

  val processorConfig = configsBuilder.processorConfig()

  val recordsFetcherFactory = {
    val f = new SimpleRecordsFetcherFactory()
    f.idleMillisBetweenCalls(
      config.idleMillisBetweenCalls.getOrElse(1500L)
    )
    f
  }

  val pollingConfig = new PollingConfig(config.streamName, config.kinesisClient)
    .maxRecords(config.maxRecords.getOrElse(1000))
    .idleTimeBetweenReadsInMillis(config.idleTimeBetweenReadsInMillis.getOrElse(configsBuilder.coordinatorConfig().shardConsumerDispatchPollIntervalMillis))
    .recordsFetcherFactory(recordsFetcherFactory)

  val retrievalConfig = configsBuilder.retrievalConfig()
    .retrievalSpecificConfig(pollingConfig)
    .initialPositionInStreamExtended(InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON))
}

case class KinesisRecordProcessorFactory(
  config: StreamConfig,
  f: Seq[Record] => Unit,
  metrics: MetricsSystem,
  logger: RollbarLogger,
) extends ShardRecordProcessorFactory {

  override def shardRecordProcessor(): ShardRecordProcessor = {
    new DefaultKinesisRecordProcessor(config, f, metrics, logger)
  }
}

class DefaultKinesisRecordProcessor(
  override val config: StreamConfig,
  f: Seq[Record] => Unit,
  override val metrics: MetricsSystem,
  override val rollbarLogger: RollbarLogger,
) extends ShardRecordProcessor with KinesisStyleRecordProcessor {

  override def initialize(input: InitializationInput): Unit =
    logger
      .withKeyValue("shard_id", input.shardId)
      .info("Initializing")

  override def processRecords(input: ProcessRecordsInput): Unit = {
    logger.withKeyValue("count", input.records.size).info("Processing records")

    streamLagMetric.update(input.millisBehindLatest)
    numRecordsMetric.update(input.records.size)

    val kinesisRecords = input.records.asScala.toSeq
    val sequenceNumbers = kinesisRecords.map(_.sequenceNumber)
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
      executeRetry(f, flowRecords, sequenceNumbers)
    }

    kinesisRecords.lastOption.foreach { record =>
      logger.withKeyValue("checkpoint", record.sequenceNumber).info("Checkpoint")
      handleCheckpoint(input.checkpointer)
    }
  }

  override def shutdownRequested(input: ShutdownRequestedInput): Unit = {
    logger.withKeyValue("reason", "requested").info("Shutting down")
    handleCheckpoint(input.checkpointer, inShutdown = true)
  }

  override def leaseLost(leaseLostInput: LeaseLostInput): Unit = {
    logger.withKeyValue("reason", "leaseLost").info("Shutting down")
  }

  override def shardEnded(input: ShardEndedInput): Unit = {
    logger.withKeyValue("reason", "shardEnded").info("Shutting down")
    handleCheckpoint(input.checkpointer, inShutdown = true)
  }

  @tailrec
  protected final def handleCheckpoint(checkpointer: RecordProcessorCheckpointer, retries: Int = 0, inShutdown: Boolean = false): Unit = {
    import KinesisStyleRecordProcessor._
    try {
      checkpointer.checkpoint()
    } catch {
      // Ignore handleCheckpoint if the processor instance has been shutdown (fail over).
      // i.e. Can't update handleCheckpoint - instance doesn't hold the lease for this shard.
      case e: ShutdownException =>
        logger.info("[FlowKinesisInfo] Caught error while checkpointing. Skipping checkpoint.", e)

      // Backoff and re-attempt handleCheckpoint upon transient failures
      // ThrottlingException | KinesisClientLibDependencyException
      case e: KinesisClientLibRetryableException =>
        if (retries >= MaxRetries) {
          val msg = s"[FlowKinesisError] Error while checkpointing after $MaxRetries attempts"
          if (inShutdown) {
            logger.info(msg, e)
          } else {
            logger.error(msg, e)
          }
        } else {
          val msg = s"[FlowKinesisWarn] Transient issue while checkpointing. Attempt ${retries + 1} of $MaxRetries."
          if (inShutdown) {
            logger.info(msg, e)
          } else {
            logger.warn(msg, e)
          }
          Thread.sleep(BackoffTimeInMillis)
          handleCheckpoint(checkpointer, retries + 1)
        }

      // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
      case e: InvalidStateException =>
        logger.error("[FlowKinesisError] Error while checkpointing. Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e)
    }
  }
}
