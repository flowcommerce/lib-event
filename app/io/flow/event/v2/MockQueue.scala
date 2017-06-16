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

  override def producer[T: TypeTag](
    numberShards: Int = 1,
    partitionKeyFieldName: String = "event_id"
  ): Producer = {
    MockProducer(stream[T])
  }

  override def consume[T: TypeTag](
    f: Record => Unit,
    pollTime: FiniteDuration = FiniteDuration(20, MILLISECONDS)
  )(
    implicit ec: ExecutionContext
  ) {
    val s = stream[T]
    val runnable = new Runnable() { override def run(): Unit = s.consume().foreach(f) }
    Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(runnable, 0, pollTime.length, pollTime.unit)
  }

  override def shutdown(implicit ec: ExecutionContext): Unit = {
    streams.clear()
  }

  override def shutdownConsumers(implicit ec: ExecutionContext): Unit = {
    // no-op
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

}

case class MockProducer(stream: MockStream) extends Producer {

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
