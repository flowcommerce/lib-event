package io.flow.event.v2

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, Executors}
import javax.inject.{Inject, Singleton}

import io.flow.event.{Record, StreamNames}
import org.joda.time.DateTime
import play.api.libs.json.JsValue

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
import scala.reflect.runtime.universe._
import scala.collection.JavaConverters._

@Singleton
class MockQueue @Inject()() extends Queue {

  private[this] val streams = new ConcurrentHashMap[String, MockStream]()
  private[this] val consumers = new ConcurrentLinkedQueue[RunningConsumer]()

  override def producer[T: TypeTag](
    numberShards: Int = 1,
    partitionKeyFieldName: String = "event_id"
  ): Producer = {
    MockProducer(stream[T])
  }

  override def consume[T: TypeTag](
    f: Seq[Record] => Unit,
    pollTime: FiniteDuration = FiniteDuration(20, MILLISECONDS)
  )(
    implicit ec: ExecutionContext
  ) {
    val s = stream[T]
    val consumer = RunningConsumer(s, f, pollTime)
    consumers.add(consumer)
  }

  override def shutdown(implicit ec: ExecutionContext): Unit = {
    shutdownConsumers
    streams.clear()
  }

  override def shutdownConsumers(implicit ec: ExecutionContext): Unit = {
    synchronized {
      consumers.asScala.foreach(_.shutdown())
      consumers.clear()
    }
  }

  def stream[T: TypeTag]: MockStream = {
    streams.computeIfAbsent(streamName[T],
      new java.util.function.Function[String, MockStream] { override def apply(s: String) = MockStream() })
  }

  private[this] def streamName[T: TypeTag] = {
    StreamNames.fromType[T] match {
      case Left(errors) => sys.error(errors.mkString(", "))
      case Right(name) => name
    }
  }

  /**
    * Clears all pending records from the queue.
    * Does not shutdown the consumers.
    */
  def clear() = streams.values().asScala.foreach(_.clearPending())

}

case class RunningConsumer(stream: MockStream, action: Seq[Record] => Unit, pollTime: FiniteDuration) {

  private val runnable = new Runnable() {
    override def run(): Unit = stream.consume().foreach(e => action(Seq(e)))
  }

  private val ses = Executors.newSingleThreadScheduledExecutor()
  ses.scheduleWithFixedDelay(runnable, 0, pollTime.length, pollTime.unit)

  def shutdown(): Unit =
    // use shutdownNow in case the provided action comes with a while-sleep loop.
    ses.shutdownNow()

}

case class MockStream() {

  private[this] val pendingRecords = new ConcurrentLinkedQueue[Record]()
  private[this] val consumedRecords = new ConcurrentLinkedQueue[Record]()

  def publish(record: Record): Unit = {
    pendingRecords.add(record)
  }

  /**
    * Consumes the next event in the stream, if any
    */
  def consume(): Option[Record] = {
    // synchronized for consistency between pending and consumed
    synchronized {
      val r = Option(pendingRecords.poll())
      r.foreach(consumedRecords.add)
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

  def clearPending() = pendingRecords.clear()
  def clearConsumed() = consumedRecords.clear()

}

case class MockProducer(stream: MockStream) extends Producer {

  override def publish[T](event: T)
                         (implicit ec: ExecutionContext, serializer:  play.api.libs.json.Writes[T]): Unit = {
    publish(serializer.writes(event))
  }

  def publish(event: JsValue)(implicit ec: ExecutionContext): Unit = {
    stream.publish(
      Record.fromJsValue(
        arrivalTimestamp = DateTime.now,
        js = event
      )
    )
  }

  def shutdown(implicit ec: ExecutionContext): Unit = {}

}
