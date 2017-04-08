package io.flow.event.v2

import javax.inject.Inject

import io.flow.event.{Record, StreamNames}
import io.flow.play.util.Config
import play.api.libs.json.JsValue

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._

trait Queue {

  def stream[T]: Stream

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

trait Stream {

  def publish(event: JsValue)(
    implicit ec: ExecutionContext
  )

  def consume(
     function: Record => Unit
  )(
     implicit ec: ExecutionContext
  )

}

class DefaultQueue @Inject() (
  config: Config
) {

  override def stream[T]: Stream = {
    val streamName = StreamNames.fromType[T] match {
      case Left(errors) => sys.error(errors.mkString(", "))
      case Right(name) => name
    }

    val flowStreamConfig = FlowConsumerConfig(
      appName = config.requiredString("name"),
      streamName = streamName,
      awsCredentialsProvider = FlowConfigAWSCredentialsProvider(config),
      function = function
    )


  }
}