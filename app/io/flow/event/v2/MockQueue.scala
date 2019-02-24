package io.flow.event.v2

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, Executors}

import io.flow.event.Record
import io.flow.log.RollbarLogger
import javax.inject.{Inject, Singleton}
import play.api.libs.json.{JsValue, Writes}

import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
import scala.reflect.runtime.universe._
import java.time.Instant

@Singleton
class MockQueue @Inject()(logger: RollbarLogger) extends Queue with StreamUsage {

  private[this] val streams = new ConcurrentHashMap[String, MockStream]()
  private[this] val consumers = new ConcurrentLinkedQueue[RunningConsumer]()

  private[this] val debug: AtomicBoolean = new AtomicBoolean(false)

  def withDebugging(): Unit = {
    debug.set(true)
  }

  override def appName: String = "io.flow.event.v2.MockQueue"

  override def producer[T: TypeTag](
    numberShards: Int = 1,
    partitionKeyFieldName: String = "event_id"
  ): Producer[T] = {
    markProducesStream(streamName[T], typeOf[T])
    MockProducer(stream[T], debug = debug.get, logger)
  }

  override def consume[T: TypeTag](
    f: Seq[Record] => Unit,
    pollTime: FiniteDuration = FiniteDuration(20, MILLISECONDS)
  ): Unit = {
    val s = stream[T]
    val consumer = RunningConsumer(s, f, pollTime)
    markConsumesStream(streamName[T], typeOf[T])
    consumers.add(consumer)
    ()
  }

  override def shutdown(): Unit = {
    shutdownConsumers()
    streams.clear()
  }

  override def shutdownConsumers(): Unit = {
    synchronized {
      consumers.asScala.foreach(_.shutdown())
      consumers.clear()
    }
  }

  def stream[T: TypeTag]: MockStream = {
    streams.computeIfAbsent(streamName[T],
      new java.util.function.Function[String, MockStream] {
        override def apply(s: String) = MockStream(s, debug = debug.get, logger)
      }
    )
  }

  /**
    * Clears all pending records from the queue.
    * Does not shutdown the consumers.
    */
  def clear(): Unit = streams.values().asScala.foreach(_.clearPending())

}

case class RunningConsumer(stream: MockStream, action: Seq[Record] => Unit, pollTime: FiniteDuration) {

  private val runnable = new Runnable() {
    override def run(): Unit = stream.consume().foreach(e => action(Seq(e)))
  }

  private val ses = Executors.newSingleThreadScheduledExecutor()
  ses.scheduleWithFixedDelay(runnable, 0, pollTime.length, pollTime.unit)

  def shutdown(): Unit = {
    // use shutdownNow in case the provided action comes with a while-sleep loop.
    ses.shutdownNow()
    ()
  }
}

case class MockStream(streamName: String, debug: Boolean = false, logger: RollbarLogger) {

  private[this] def logDebug(f: => String): Unit = {
    if (debug) {
      logger.withKeyValue("class", getClass.getName).info(f)
    }
  }

  private[this] val pendingRecords = new ConcurrentLinkedQueue[Record]()
  private[this] val consumedRecords = new ConcurrentLinkedQueue[Record]()

  def publish(record: Record): Unit = {
    logDebug { s"publishing record: ${record.js}" }
    pendingRecords.add(record)
    ()
  }

  /**
    * Consumes the next event in the stream, if any
    */
  def consume(): Option[Record] = {
    // synchronized for consistency between pending and consumed
    synchronized {
      logDebug { s"consume() starting" }
      val r = Option(pendingRecords.poll())
      r.foreach { rec =>
        logDebug { s"Consumed record: $rec" }
        consumedRecords.add(rec)
      }
      r
    }
  }

  /**
    * Finds the event w/ the specified id. Returns none if
    * we have not yet received this event.
    */
  def findByEventId(eventId: String): Option[Record] = {
    all.find(_.eventId == eventId)
  }

  /**
    * Returns all records seen - pending and consumed
    */
  def all: Seq[Record] = {
    // synchronized for consistency between pending and consumed
    synchronized {
      (pendingRecords.asScala ++ consumedRecords.asScala).toSeq
    }
  }

  def pending: Seq[Record] = pendingRecords.asScala.toSeq
  def consumed: Seq[Record] = consumedRecords.asScala.toSeq

  def clearPending(): Unit = pendingRecords.clear()
  def clearConsumed(): Unit = consumedRecords.clear()

}

case class MockProducer[T](stream: MockStream, debug: Boolean = false, logger: RollbarLogger) extends Producer[T] with StreamUsage {

  private[this] def logDebug(f: => String): Unit = {
    if (debug) {
      logger
        .withKeyValue("class", getClass.getName)
        .info(f)
    }
  }

  private def publish(event: JsValue): Unit = {
    val r= Record.fromJsValue(
      arrivalTimestamp = Instant.now,
      js = event
    )

    logDebug { s"Publishing event: $event" }
    stream.publish(
      r
    )
  }

  override def publish[U <: T](event: U)(implicit serializer: Writes[U]): Unit = {
    val w = serializer.writes(event)
    markProducedEvent(stream.streamName, w)
    publish(w)
  }

  def shutdown(): Unit = {
    logDebug { "shutting down" }
  }

}
