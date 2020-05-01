package io.flow.event.v2

import com.amazonaws.auth.AWSCredentialsProviderChain
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder
import com.amazonaws.services.dynamodbv2.streamsadapter.StreamsWorkerFactory
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput
import io.flow.event.Record
import io.flow.log.RollbarLogger
import io.flow.play.metrics.MetricsSystem

import scala.jdk.CollectionConverters._

case class DynamoStreamConsumer(
  config: DynamoStreamConfig,
  creds: AWSCredentialsProviderChain,
  f: Seq[Record] => Unit,
  metrics: MetricsSystem,
  logger: RollbarLogger
) extends KinesisConsumer(
  config = config,
  logger = logger,
  worker = StreamsWorkerFactory.createDynamoDbStreamsWorker(
    new DynamoStreamRecordProcessorFactory(
      config = config,
      f = f,
      metrics= metrics,
      logger = logger
    ),
    config.toKclConfig(creds),
    config.kinesisClient,
    config.dynamoDBClient,
    AmazonCloudWatchClientBuilder.defaultClient()
  )
)

class DynamoStreamRecordProcessorFactory(
  config: StreamConfig,
  f: Seq[Record] => Unit,
  metrics: MetricsSystem,
  logger: RollbarLogger
) extends IRecordProcessorFactory {

  override def createProcessor(): IRecordProcessor = new DynamoStreamRecordProcessor(
    config = config,
    f = f,
    metrics = metrics,
    logger = logger
  )
}

class DynamoStreamRecordProcessor(
  config: StreamConfig,
  f: Seq[Record] => Unit,
  metrics: MetricsSystem,
  logger: RollbarLogger
) extends KinesisRecordProcessor(config, f, metrics, logger) {

  private val recordType = config.eventClass

  override def processRecords(input: ProcessRecordsInput): Unit = {
    logger_.withKeyValue("count", input.getRecords.size).info("Processing records")

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
            logger_.withKeyValue("record_type", record.getClass.getName).warn("Unknown record type")
            None
        }
      }
      executeRetry(flowRecords, sequenceNumbers)
    }

    kinesisRecords.lastOption.foreach { record =>
      logger_.withKeyValue("checkpoint", record.getSequenceNumber).info("Checkpoint")
      handleCheckpoint(input.getCheckpointer)
    }
  }
}