package io.flow.event.v2

import javax.inject.{Inject, Singleton}

import io.flow.event.{Record, StreamNames}
import org.joda.time.DateTime
import play.api.libs.json.JsValue

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.reflect.runtime.universe._

@Singleton
class MockQueue @Inject()() extends Queue {

  private[this] val streams = scala.collection.mutable.Map[String, MockStream]()

  override def producer[T: TypeTag](
    numberShards: Int = 1,
    partitionKeyFieldName: String = "event_id"
  ): Producer = {
    MockProducer(stream[T])
  }

  override def consume[T: TypeTag](
    f: Record => Unit,
    pollTime: FiniteDuration = FiniteDuration(5, "seconds")
  )(
    implicit ec: ExecutionContext
  ) {
    stream[T].consume().foreach(f)
  }

  override def shutdown(implicit ec: ExecutionContext): Unit = {
    streams.clear()
  }

  override def shutdownConsumers(implicit ec: ExecutionContext): Unit = {
    // no-op
  }


  def stream[T: TypeTag]: MockStream = {
    streams.get(streamName[T]).getOrElse {
      val stream = MockStream()
      streams.put(streamName[T], stream)
      stream
    }
  }

  private[this] def streamName[T: TypeTag] = {
    StreamNames.fromType[T] match {
      case Left(errors) => sys.error(errors.mkString(", "))
      case Right(name) => name
    }
  }

}

case class MockStream() {

  private[this] val pendingRecords = scala.collection.mutable.ListBuffer[Record]()
  private[this] val consumedRecords = scala.collection.mutable.ListBuffer[Record]()

  def publish(record: Record): Unit = {
    pendingRecords.append(record)
  }

  /**
    * Consumes the next event in the stream, if any
    */
  def consume(): Option[Record] = {
    pendingRecords.headOption.map { r =>
      pendingRecords.remove(0)
      consumedRecords.append(r)
      r
    }
  }

  /**
    * Consumes the event w/ the specified id. Returns none if
    * we have not yet received this event.
    */
  def consumeEventId(eventId: String): Option[Record] = {
    pendingRecords.find(_.eventId == eventId).map { rec =>
      val i = pendingRecords.indexOf(rec)
      pendingRecords.remove(i)
      consumedRecords.append(rec)
      rec
    }
  }

  /**
    * Returns all records seen - pending and consumed
    */
  def all: Seq[Record] = {
    pendingRecords ++ consumedRecords
  }

  def pending: Seq[Record] = pendingRecords
  def consumed: Seq[Record] = consumedRecords

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
