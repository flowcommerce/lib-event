package io.flow.event.v2

import java.util.concurrent.ConcurrentLinkedQueue
import javax.inject.Inject

import com.amazonaws.auth.BasicAWSCredentials
import io.flow.event.{Record, StreamNames}
import io.flow.play.util.Config
import play.api.libs.json.JsValue

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.reflect.runtime.universe._

trait Queue {

  def producer[T: TypeTag](
    numberShards: Int = 1,
    partitionKeyFieldName: String = "event_id"
  ): Producer

  /**
    * Creates a thread that will poll kinesis on the specified interval,
    * invoking your provided function for each new record
    */
  def consume[T: TypeTag](
    f: Record => Unit,
    pollTime: FiniteDuration = FiniteDuration(5, "seconds")
  )(
    implicit ec: ExecutionContext
  )

  def shutdown(implicit ec: ExecutionContext)

  def shutdownConsumers(implicit ec: ExecutionContext)

}

trait Producer {

  def publish(event: JsValue)(implicit ec: ExecutionContext): Unit

  def publishBatch(events: Seq[JsValue])(implicit ec: ExecutionContext): Unit = events.foreach(publish)

  def shutdown(implicit ec: ExecutionContext): Unit

}

/**
  * Builds our default producer/consumer. Requires the following config
  * variables to be set:
  *   - aws.access.key
  *   - aws.secret.key
  */
class DefaultQueue @Inject() (
  config: Config
) extends Queue {

  import scala.collection.JavaConverters._

  private[this] val consumers = new ConcurrentLinkedQueue[KinesisConsumer]()

  override def producer[T: TypeTag](
    numberShards: Int = 1,
    partitionKeyFieldName: String = "event_id"
  ): Producer = {
    KinesisProducer(
      streamConfig[T],
      numberShards,
      partitionKeyFieldName
    )
  }

  override def consume[T: TypeTag](
     f: Record => Unit,
     pollTime: FiniteDuration = FiniteDuration(5, "seconds")
  )(
     implicit ec: ExecutionContext
  ) {
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

  private[this] def awsCredentials = new BasicAWSCredentials(
    config.requiredString("aws.access.key"),
    config.requiredString("aws.secret.key")
  )

  private[this] def streamConfig[T: TypeTag] = {
    StreamConfig(
      awsCredentials,
      appName = config.requiredString("name"),
      streamName = streamName[T]
    )
  }
}