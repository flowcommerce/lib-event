package io.flow.event.v2

import javax.inject.Inject

import io.flow.event.{Record, StreamNames}
import io.flow.play.util.Config
import play.api.libs.json.JsValue

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._

trait Queue {

  def producer[T]: Producer

  def consumer[T](
    function: Record => Unit
  ): Consumer

}

trait Producer {

  def publish(event: JsValue)

}

trait Consumer {

  def consume(
    function: Record => Unit
  )(
    implicit ec: ExecutionContext
  )

}

class DefaultQueue @Inject() (
  config: Config
) extends Queue {

  override def producer[T]: Producer = {
    KinesisProducer(config, streamName)
  }

  override def consumer[T](
    function: Record => Unit
  ): Consumer = {
    KinesisConsumer(
      KinesisConsumerConfig(
        appName = config.requiredString("name"),
        streamName = streamName,
        awsCredentialsProvider = FlowConfigAWSCredentialsProvider(config),
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
}