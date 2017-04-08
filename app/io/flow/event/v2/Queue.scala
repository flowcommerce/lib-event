package io.flow.event.v2

import javax.inject.Inject

import com.amazonaws.auth.BasicAWSCredentials
import io.flow.event.{Record, StreamNames}
import io.flow.play.util.Config
import play.api.libs.json.JsValue

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._

trait Queue {

  def producer[T: TypeTag]: Producer

  def consumer[T: TypeTag](
    function: Record => Unit
  ): Consumer

}

trait Producer {

  def publish(event: JsValue)(implicit ec: ExecutionContext)

}

trait Consumer {

  def consume(implicit ec: ExecutionContext)

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

  override def producer[T: TypeTag]: Producer = {
    KinesisProducer(awsCredentials, streamName[T])
  }

  override def consumer[T: TypeTag](
    function: Record => Unit
  ): Consumer = {
    KinesisConsumer(
      KinesisConsumerConfig(
        appName = config.requiredString("name"),
        streamName = streamName[T],
        awsCredentials = awsCredentials,
        function = function
      )
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

}