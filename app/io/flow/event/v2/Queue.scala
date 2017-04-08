package io.flow.event.v2

import javax.inject.Inject

import com.amazonaws.auth.BasicAWSCredentials
import io.flow.event.{Record, StreamNames}
import io.flow.play.util.Config
import play.api.libs.json.JsValue

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._

trait Queue {

  def producer[T: TypeTag](
    numberShards: Int = 1,
    partitionKeyFieldName: String = "event_id"
  ): Producer


  def consumer[T: TypeTag]: Consumer

}

trait Producer {

  def publish(event: JsValue)(implicit ec: ExecutionContext)

  def shutdown(implicit ec: ExecutionContext)

}

trait Consumer {

  def consume(f: Record => Unit)

  def shutdown(implicit ec: ExecutionContext)

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

  override def consumer[T: TypeTag]: Consumer = {
    KinesisConsumer(
      streamConfig[T]
    )
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