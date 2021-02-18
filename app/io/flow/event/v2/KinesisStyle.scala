package io.flow.event.v2

import com.codahale.metrics.Histogram
import io.flow.event.Record
import io.flow.log.RollbarLogger
import io.flow.play.metrics.MetricsSystem

import java.util.concurrent.Executors
import scala.annotation.tailrec
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

abstract class KinesisStyleConsumer(
  config: StreamConfig,
  rollbarLogger: RollbarLogger,
) {

  protected val logger =
    rollbarLogger
      .fingerprint(getClass.getName)
      .withKeyValue("stream", config.streamName)
      .withKeyValue("worker_id", config.workerId)

  protected val executor = Executors.newSingleThreadExecutor()

  def start(): Unit
  def shutdown(): Unit
}

object KinesisStyleRecordProcessor {
  // Yes, it is arbitrary
  val MaxRetries = 8
  val BackoffTimeInMillis = 3000L
}

trait KinesisStyleRecordProcessor {
  def config: StreamConfig
  def metrics: MetricsSystem
  def rollbarLogger: RollbarLogger

  import KinesisStyleRecordProcessor._

  protected val streamLagMetric: Histogram = metrics.registry.histogram(s"${config.streamName}.consumer.lagMillis")
  protected val numRecordsMetric: Histogram = metrics.registry.histogram(s"${config.streamName}.consumer.numRecords")

  protected val logger = rollbarLogger
    .fingerprint(this.getClass.getName)
    .withKeyValue("class", this.getClass.getName)
    .withKeyValue("stream", config.streamName)
    .withKeyValue("worker_id", config.workerId)

  @tailrec
  protected final def executeRetry(f: Seq[Record] => Unit, records: Seq[Record], sequenceNumbers: Seq[String], retries: Int = 0): Unit = {
    Try(f(records)) match {
      case Success(_) =>
      case Failure(NonFatal(e)) =>
        if (retries >= MaxRetries) {
          val size = records.size
          logger
            .withKeyValue("retries", retries)
            .error(s"[FlowKinesisError] Error while processing records after $MaxRetries attempts. " +
              s"$size records are skipped. Sequence numbers: ${sequenceNumbers.mkString(", ")}", e)
        } else {
          logger
            .withKeyValue("retries", retries)
            .warn(s"[FlowKinesisWarn] Error while processing records (retry $retries/$MaxRetries). Retrying...", e)
          Thread.sleep(BackoffTimeInMillis)
          executeRetry(f, records, sequenceNumbers, retries + 1)
        }
      case Failure(e) =>
        throw e
    }
  }

}

