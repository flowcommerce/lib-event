package io.flow.event.v2

import java.net.InetAddress
import java.util.UUID
import java.util.concurrent.Executors

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput}
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel
import io.flow.event.Record
import io.flow.play.util.FlowEnvironment
import org.joda.time.DateTime
import play.api.Logger

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

case class KinesisConsumer (
  config: StreamConfig,
  f: Seq[Record] => Unit
) {

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
    .recordProcessorFactory(KinesisRecordProcessorFactory(config, workerId, f))
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
        .withIdleTimeBetweenReadsInMillis(config.idleTimeBetweenReadsInMillis)
        .withMaxRecords(config.maxRecords)
        .withMetricsLevel(MetricsLevel.DETAILED)
        .withFailoverTimeMillis(10000) // See https://github.com/awslabs/amazon-kinesis-connectors/issues/10
    ).kinesisClient(config.kinesisClient)
    .build()

  private[this] val exec = Executors.newSingleThreadExecutor()

  Logger.info(s"[${this.getClass.getName}] Creating KinesisConsumer for app[${config.appName}] stream[${config.streamName}] workerId[$workerId]")

  exec.execute(worker)

  def shutdown(implicit ec: ExecutionContext): Unit = {
    exec.shutdown()
  }

}

case class KinesisRecordProcessorFactory(
  config: StreamConfig,
  workerId: String,
  f: Seq[Record] => Unit
) extends IRecordProcessorFactory {

  override def createProcessor(): IRecordProcessor = {
    KinesisRecordProcessor(config, workerId, f)
  }

}

case class KinesisRecordProcessor[T](
  config: StreamConfig,
  workerId: String,
  f: Seq[Record] => Unit
) extends IRecordProcessor {

  override def initialize(input: InitializationInput): Unit = {
    Logger.info(s"KinesisRecordProcessor workerId[$workerId] initializing stream[${config.streamName}] shard[${input.getShardId}]")
  }

  override def processRecords(input: ProcessRecordsInput): Unit = {
    Logger.info(s"KinesisRecordProcessor workerId[$workerId] processRecords  stream[${config.streamName}] starting")
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

    f(flowRecords)


    kinesisRecords.lastOption.foreach { record =>
      Logger.info(s"KinesisRecordProcessor workerId[$workerId] checkpoint(${record.getSequenceNumber})")
      input.getCheckpointer.checkpoint(record)
    }
  }

  override def shutdown(input: ShutdownInput): Unit = {
    Logger.info(s"shutting down stream[${config.streamName}] reason[${input.getShutdownReason}]")
  }

}
