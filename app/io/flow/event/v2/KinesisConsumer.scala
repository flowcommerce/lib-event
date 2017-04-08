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
  config: StreamConfig,
  f: Record => Unit
) {

  private[this] val kinesisClient = config.kinesisClient

  private[this] val workerId = Seq(
    config.appName,
    InetAddress.getLocalHost.getCanonicalHostName,
    UUID.randomUUID.toString
  ).mkString(":")

  private[this] val worker = new Worker.Builder()
    .recordProcessorFactory(KinesisRecordProcessorFactory(config, workerId, f))
    .config(
      new KinesisClientLibConfiguration(
        config.appName,
        config.streamName,
        config.awSCredentialsProvider,
        workerId
      ).withTableName(dynamoTableName)
        .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
        .withCleanupLeasesUponShardCompletion(true)
        .withIdleTimeBetweenReadsInMillis(config.idleTimeBetweenReadsInMillis)
        .withShardSyncIntervalMillis(5000)
    ).kinesisClient(kinesisClient)
    .build()

  private[this] val exec = Executors.newSingleThreadExecutor()

  println(s"Starting worker[$workerId]")
    /*
    exec.execute(new Runnable {
      override def run(): Unit = {
        worker.run()
      }
    })
    */
    exec.execute(worker)

  def shutdown(implicit ec: ExecutionContext): Unit = {
    worker.shutdown()
    exec.shutdown()
  }

  private[this] def dynamoTableName: String = {
    Seq(
      Naming.envPrefix,
      "kinesis",
      config.appName
    ).mkString(".")
  }

}

case class KinesisRecordProcessorFactory(
  config: StreamConfig,
  workerId: String,
  f: Record => Unit
) extends IRecordProcessorFactory {

  override def createProcessor(): IRecordProcessor = {
    KinesisRecordProcessor(config, workerId, f: Record => Unit)
  }

}

case class KinesisRecordProcessor[T](
  config: StreamConfig,
  workerId: String,
  f: Record => Unit
) extends IRecordProcessor {

  override def initialize(input: InitializationInput): Unit = {
    //Logger.info(s"KinesisRecordProcessor[$workerId] initializing stream[${config.streamName}] shard[${input.getShardId}]")
  }

  override def processRecords(input: ProcessRecordsInput): Unit = {
    //Logger.info(s"KinesisRecordProcessor[$workerId] processRecords  stream[${config.streamName}] starting")
    val all = input.getRecords.asScala
    all.foreach { record =>
      val buffer = record.getData
      val bytes = Array.fill[Byte](buffer.remaining)(0)
      buffer.get(bytes)

      println(s"processRecord[$workerId]: " + Record.fromByteArray(
        arrivalTimestamp = new DateTime(record.getApproximateArrivalTimestamp),
        value = bytes
      ))

      f(
        Record.fromByteArray(
          arrivalTimestamp = new DateTime(record.getApproximateArrivalTimestamp),
          value = bytes
        )
      )

    }

    all.lastOption.foreach { record =>
      //Logger.info(s"KinesisRecordProcessor[$workerId] checkpoint(${record.getSequenceNumber})")
      input.getCheckpointer.checkpoint(record)
    }
  }

  override def shutdown(input: ShutdownInput): Unit = {
    //Logger.info(s"shutting down stream[${config.streamName}] reason[${input.getShutdownReason}]")
  }

}
