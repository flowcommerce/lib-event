package io.flow.event.v2

import java.net.InetAddress
import java.util.UUID
import java.util.concurrent.{ExecutorService, Executors}

import io.flow.event.{Naming, Record}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput}
import org.joda.time.DateTime
import play.api.Logger

import scala.concurrent.ExecutionContext
import collection.JavaConverters._

case class KinesisConsumer (
  config: StreamConfig,
  f: Record => Unit
) {

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
      ).withTableName(Naming.dynamoKinesisTableName(config.appName))
        .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
        .withCleanupLeasesUponShardCompletion(true)
        .withIdleTimeBetweenReadsInMillis(config.idleTimeBetweenReadsInMillis)
        .withShardSyncIntervalMillis(5000)
    ).kinesisClient(config.kinesisClient)
    .build()

  private[this] val exec = Executors.newSingleThreadExecutor()

  Logger.info(s"[${this.getClass.getName}] Creating KinesisConsumer for app[${config.appName}] stream[${config.streamName}] workerId[$workerId]")

  exec.execute(worker)

  def shutdown(implicit ec: ExecutionContext): Unit = {
    worker.shutdown()
    exec.shutdown()
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

      val rec = Record.fromByteArray(
        arrivalTimestamp = new DateTime(record.getApproximateArrivalTimestamp),
        value = bytes
      )

      Logger.info(s"KinesisRecordProcessor[$workerId] eventId[${rec.eventId}] - CALLING FUNCTION")
      f(rec)
      Logger.info(s"KinesisRecordProcessor[$workerId] eventId[${rec.eventId}] - DONE")
    }

    all.lastOption.foreach { record =>
      Logger.info(s"KinesisRecordProcessor[$workerId] checkpoint(${record.getSequenceNumber})")
      input.getCheckpointer.checkpoint(record)
    }
  }

  override def shutdown(input: ShutdownInput): Unit = {
    //Logger.info(s"shutting down stream[${config.streamName}] reason[${input.getShutdownReason}]")
  }

}
