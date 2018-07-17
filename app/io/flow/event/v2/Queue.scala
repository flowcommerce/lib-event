package io.flow.event.v2

import java.util.concurrent.ConcurrentLinkedQueue
import javax.inject.Inject

import io.flow.event.Record
import io.flow.util.StreamNames
import io.flow.play.util.Config

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.reflect.runtime.universe._

trait Queue {

  def producer[T: TypeTag](
    numberShards: Int = 1,
    partitionKeyFieldName: String = "event_id"
  ): Producer[T]

  /**
    * Creates a thread that will poll kinesis on the specified interval,
    * invoking your provided function for each new record
    */
  def consume[T: TypeTag](
    f: Seq[Record] => Unit,
    pollTime: FiniteDuration = FiniteDuration(5, "seconds")
  )(implicit ec: ExecutionContext)

  def shutdown(implicit ec: ExecutionContext)

  def shutdownConsumers(implicit ec: ExecutionContext)

}

trait Producer[T] {

  def publish[U <: T](event: U)(implicit ec: ExecutionContext, serializer: play.api.libs.json.Writes[U]): Unit

  def publishBatch[U <: T](events: Seq[U])
                     (implicit ec: ExecutionContext, serializer: play.api.libs.json.Writes[U]): Unit =
    events.foreach(publish[U])

  def shutdown(implicit ec: ExecutionContext): Unit

}


/**
  * Builds our default producer/consumer
  */
class DefaultQueue @Inject() (
  config: Config,
  creds: AWSCreds
) extends Queue with StreamUsage {

  import scala.collection.JavaConverters._

  private[this] val consumers = new ConcurrentLinkedQueue[KinesisConsumer]()

  override def producer[T: TypeTag](
    numberShards: Int = 1,
    partitionKeyFieldName: String = "event_id"
  ): Producer[T] = {
    markProduced[T]()
    KinesisProducer(
      streamConfig[T],
      numberShards,
      partitionKeyFieldName
    )
  }

  override def consume[T: TypeTag](
     f: Seq[Record] => Unit,
     pollTime: FiniteDuration = FiniteDuration(5, "seconds")
  )(implicit ec: ExecutionContext) {
    markConsumed[T]()
    consumers.add(
      KinesisConsumer(
        streamConfig[T],
        f
      )
    )
  }

  override def shutdownConsumers(implicit ec: ExecutionContext): Unit = {
    // synchronized to avoid a consumer being registered "in between" shutdown and clear
    synchronized {
      consumers.asScala.foreach(_.shutdown)
      consumers.clear()
    }
  }

  override def shutdown(implicit ec: ExecutionContext): Unit = {
    shutdownConsumers
  }

  private[this] def streamName[T: TypeTag]: String = {
    StreamNames.fromType[T] match {
      case Left(errors) => sys.error(errors.mkString(", "))
      case Right(name) => name
    }
  }

  private[this] def streamConfig[T: TypeTag] = {
    DefaultStreamConfig(
      creds,
      appName = config.requiredString("name"),
      streamName = streamName[T]
    )
  }
}