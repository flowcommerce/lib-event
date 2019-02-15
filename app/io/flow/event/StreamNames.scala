package io.flow.event

import io.flow.util.FlowEnvironment

import scala.reflect.runtime.universe._

@deprecated("Deprecated in favour of lib-util (io.flow.util.*)", "0.3.45")
case class ApidocClass(
  namespace: String,
  service: String,
  version: Int,
  name: String
) {

  val namespaces: Seq[String] = service.split("\\.")

}

@deprecated("Deprecated in favour of lib-util (io.flow.util.*)", "0.3.45")
object Naming {

  @deprecated("Deprecated in favour of lib-util (io.flow.util.*)", "0.3.45")
  def dynamoKinesisTableName(streamName: String, appName: String): String = {
    Seq(
      "kinesis",
      streamName,
      appName
    ).mkString(".")
  }

  /**
    * Returns either 'production' or 'development_workstation' based on the
    * flow environment
    */
  @deprecated("Deprecated in favour of lib-util (io.flow.util.*)", "0.3.45")
  def envPrefix(env: FlowEnvironment = FlowEnvironment.Current): String = {
    env match {
      case FlowEnvironment.Production => "production"
      case FlowEnvironment.Development | FlowEnvironment.Workstation => "development_workstation"
    }
  }

}

@deprecated("Deprecated in favour of lib-util (io.flow.util.*)", "0.3.45")
object StreamNames {

  private[this] val ApidocClassRegexp = """^(io\.flow)\.([a-z]+(\.[a-z]+)*)\.v(\d+)\.models\.(\w+)$""".r

  @deprecated("Deprecated in favour of lib-util (io.flow.util.*)", "0.3.45")
  def parse(name: String): Option[ApidocClass] = {
    name match {
      case ApidocClassRegexp(namespace, service, _, version, n) => {
        Some(
          ApidocClass(
            namespace = namespace,
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

  @deprecated("Deprecated in favour of lib-util (io.flow.util.*)", "0.3.45")
  def toSnakeCase(name: String): String = {
    name.replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2").replaceAll("([a-z\\d])([A-Z])", "$1_$2").toLowerCase
  }

  /**
    * Returns the stream name based on the type of the class (a Right), or a validation
    * error if the class name if invalid (a Left)
    */
  @deprecated("Deprecated in favour of lib-util (io.flow.util.*)", "0.3.45")
  def fromType[T: TypeTag]: Either[Seq[String], String] = {
    val name = typeOf[T].toString

    StreamNames(FlowEnvironment.Current).json(name) match {
      case None => {
        name match {
          case "Any" | "Nothing" => {
            Left(Seq(s"FlowKinesisError Stream[$name] In order to consume events, you must annotate the type you are expecting as this is used to build the stream. Type should be something like io.flow.user.v0.unions.SomeEvent"))
          }
          case _ => {
            Left(Seq(s"FlowKinesisError Stream[$name] Could not parse stream name. Expected something like io.flow.user.v0.unions.SomeEvent"))
          }
        }
      }

      case Some(name) => {
        Right(name)
      }
    }
  }
}

@deprecated("Deprecated in favour of lib-util (io.flow.util.*)", "0.3.45")
case class StreamNames(env: FlowEnvironment) {

  /**
    * Turns a full class name into the name of a kinesis stream
    */
  @deprecated("Deprecated in favour of lib-util (io.flow.util.*)", "0.3.45")
  def json(className: String): Option[String] = {
    StreamNames.parse(className).map { apidoc =>
      s"${Naming.envPrefix(env)}.${apidoc.service}.v${apidoc.version}.${apidoc.name}.json"
    }
  }

}
