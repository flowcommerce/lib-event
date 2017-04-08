package io.flow.event.v2

import java.net.InetAddress
import java.util.UUID

import com.amazonaws.auth.{AWSCredentials, AWSStaticCredentialsProvider}
import io.flow.event.Record
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput}
import org.joda.time.DateTime

import scala.concurrent.ExecutionContext
import collection.JavaConverters._

case class KinesisConsumer (
  config: StreamConfig
) extends Consumer {

  private[this] val workerId = InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID

  private[this] val worker = new Worker.Builder()
    .recordProcessorFactory(
      KinesisRecordProcessorFactory(config, { rec => process(rec) })
    )
    .config(
      new KinesisClientLibConfiguration(
        config.appName,
        config.streamName,
        new AWSStaticCredentialsProvider(config.awsCredentials),
        workerId
      ).withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
    )
    .build()

  private[this] def process(record: Record): Unit = {
    println(s"GOT REC: $record")
  }

  private[this] var isShutdown: Boolean = false

  override def shutdown(implicit ec: ExecutionContext): Unit = {
    worker.shutdown()
    this.isShutdown = true
  }

  override def consume(f: Record => Unit)(implicit ec: ExecutionContext) {
    worker.run()
  }
}

case class KinesisRecordProcessorFactory(config: StreamConfig, f: Record => Unit) extends IRecordProcessorFactory {

  override def createProcessor(): IRecordProcessor = {
    KinesisRecordProcessor(config, f: Record => Unit)
  }

}

case class KinesisRecordProcessor[T](
  config: StreamConfig,
  f: Record => Unit
) extends IRecordProcessor {

  override def initialize(input: InitializationInput): Unit = {
    println(s"initializing stream[${config.streamName}] shard[${input.getShardId}]")
  }

  override def processRecords(input: ProcessRecordsInput): Unit = {
    println(s"processRecords  stream[${config.streamName}] starting")
    input.getRecords.asScala.foreach { record =>
      val buffer = record.getData
      val bytes = Array.fill[Byte](buffer.remaining)(0)
      buffer.get(bytes)

      val flowRecord = Record.fromByteArray(
        arrivalTimestamp = new DateTime(record.getApproximateArrivalTimestamp),
        value = bytes
      )

      println(s"processRecords  stream[${config.streamName}] flowRecord: $flowRecord")
      f(flowRecord)
    }
  }

  override def shutdown(input: ShutdownInput): Unit = {
    println(s"shutting down stream[${config.streamName}] reason[${input.getShutdownReason}]")
  }

}
