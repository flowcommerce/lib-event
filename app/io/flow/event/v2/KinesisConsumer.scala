package io.flow.event.v2

import java.net.InetAddress
import java.util.UUID
import java.util.concurrent.{ExecutorService, Executors}

import io.flow.event.{Naming, Record}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput}
import org.joda.time.DateTime

import scala.concurrent.ExecutionContext
import collection.JavaConverters._

case class KinesisConsumer (
  config: StreamConfig
) extends Consumer {

  private[this] val kinesisClient = config.kinesisClient

  private[this] val threadPools = scala.collection.mutable.ListBuffer[ExecutorService]()
  private[this] val workers = scala.collection.mutable.ListBuffer[Worker]()

  override def consume(f: Record => Unit) {
    val workerId = Seq(
      config.appName,
      InetAddress.getLocalHost.getCanonicalHostName,
      UUID.randomUUID.toString
    ).mkString(":")

    val worker = new Worker.Builder()
      .recordProcessorFactory(KinesisRecordProcessorFactory(config, f))
      .config(
        new KinesisClientLibConfiguration(
          config.appName,
          config.streamName,
          config.awSCredentialsProvider,
          workerId
        ).withTableName(dynamoTableName)
          .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
      ).kinesisClient(kinesisClient)
      .build()

    Executors.newSingleThreadExecutor().execute(new Runnable {
      override def run(): Unit = {
        worker.run()
      }
    })
  }

  override def shutdown(implicit ec: ExecutionContext): Unit = {
    workers.foreach { _.shutdown() }
    workers.clear()

    threadPools.foreach { _.shutdown() }
    threadPools.clear()
  }

  private[this] def dynamoTableName: String = {
    Seq(
      Naming.envPrefix,
      "kinesis",
      config.appName
    ).mkString(".")
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
    //Logger.info(s"initializing stream[${config.streamName}] shard[${input.getShardId}]")
  }

  override def processRecords(input: ProcessRecordsInput): Unit = {
    //Logger.info(s"processRecords  stream[${config.streamName}] starting")
    val all = input.getRecords.asScala
    all.foreach { record =>
      val buffer = record.getData
      val bytes = Array.fill[Byte](buffer.remaining)(0)
      buffer.get(bytes)

      f(
        Record.fromByteArray(
          arrivalTimestamp = new DateTime(record.getApproximateArrivalTimestamp),
          value = bytes
        )
      )

    }

    all.lastOption.foreach { record =>
      //Logger.info(s"input.getCheckpointer.checkpoint(${record.getSequenceNumber})")
      input.getCheckpointer.checkpoint(record)
    }
  }

  override def shutdown(input: ShutdownInput): Unit = {
    //Logger.info(s"shutting down stream[${config.streamName}] reason[${input.getShutdownReason}]")
  }

}
