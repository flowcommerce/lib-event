package io.flow.event.v2

import java.net.InetAddress
import java.util.UUID
import javax.inject.Inject

import com.amazonaws.auth.AWSCredentialsProvider
import io.flow.event.{Record, StreamNames}
import io.flow.play.util.Config
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput}
import org.joda.time.DateTime

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._
import collection.JavaConverters._

class KinesisConsumer (
  config: FlowConsumerConfig
) extends Consumer {

  override def consume(
    function: Record => Unit
  )(
    implicit ec: ExecutionContext
  ) {
    val workerId = InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID

    val kinesisConfig = new KinesisClientLibConfiguration(
      flowStreamConfig.appName,
      flowStreamConfig.streamName,
      flowStreamConfig.awsCredentialsProvider,
      workerId
    ).withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)

    new Worker.Builder()
      .recordProcessorFactory(KinesisRecordProcessorFactory(flowStreamConfig))
      .config(kinesisConfig)
      .build()
      .run()
  }
}

case class FlowConsumerConfig(
  awsCredentialsProvider: AWSCredentialsProvider,
  appName: String,
  streamName: String,
  function: Record => Unit
)

case class KinesisRecordProcessorFactory(config: FlowConsumerConfig) extends IRecordProcessorFactory {

  override def createProcessor(): IRecordProcessor = {
    KinesisRecordProcessor(config)
  }

}

case class KinesisRecordProcessor[T](
  config: FlowConsumerConfig
) extends IRecordProcessor {

  override def initialize(input: InitializationInput): Unit = {
    println(s"initializing stream[${config.streamName}] shard[${input.getShardId}]")
  }

  override def processRecords(input: ProcessRecordsInput): Unit = {
    println("processRecords  stream[${config.streamName}] starting")
    input.getRecords.asScala.foreach { record =>
      val buffer = record.getData
      val bytes = Array.fill[Byte](buffer.remaining)(0)
      buffer.get(bytes)

      val flowRecord = Record.fromByteArray(
        arrivalTimestamp = new DateTime(record.getApproximateArrivalTimestamp),
        value = bytes
      )

      println("processRecords  stream[${config.streamName}] flowRecord: $flowRecord")
      config.function(flowRecord)
    }
  }

  override def shutdown(input: ShutdownInput): Unit = {
    println(s"shutting down stream[${config.streamName}] reason[${input.getShutdownReason}]")
  }

}
