package io.flow.event

import io.flow.play.util.FlowEnvironment

case class StreamNames(env: FlowEnvironment) {

  private[this] val ApidocClass = "^io\\.flow\\.([a-z]+(\\.[a-z]+)*)\\.(v\\d+)\\.models\\.(\\w+)$".r

  /**
    * Turns a full class name into the name of a kinesis stream
    */
  def json(className: String): Option[String] = {
    className match {
      case ApidocClass(service, placeholder, version, className) => {
        val snakeClassName = toSnakeCase(className)
        Some(s"$env.$service.$version.$snakeClassName.json")
      }

      case _ => {
        None
      }

    }

  }

  def toSnakeCase(name: String): String = {
    name.replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2").replaceAll("([a-z\\d])([A-Z])", "$1_$2").toLowerCase
  }

}
