package io.flow.event

import io.flow.play.util.FlowEnvironment

case class ApidocClass(
  service: String,
  version: Int,
  name: String
)

object StreamNames {

  private[this] val ApidocClassRegexp = """^io\.flow\.([a-z]+(\.[a-z]+)*)\.v(\d+)\.models\.(\w+)$""".r
  
  def parse(name: String): Option[ApidocClass] = {
    name match {
      case ApidocClassRegexp(service, _, version, n) => {
        Some(
          ApidocClass(
            service = service,
            version = version.toInt,
            name = toSnakeCase(n)
          )
        )
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

case class StreamNames(env: FlowEnvironment) {

  private[this] val streamEnv = env match {
    case FlowEnvironment.Production => "production"
    case FlowEnvironment.Development | FlowEnvironment.Workstation => "development_workstation"
  }

  /**
    * Turns a full class name into the name of a kinesis stream
    */
  def json(className: String): Option[String] = {
    StreamNames.parse(className).map { apidoc =>
      s"$streamEnv.${apidoc.service}.v${apidoc.version}.${apidoc.name}.json"
    }
  }


}
