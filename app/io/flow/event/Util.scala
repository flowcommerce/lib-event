package io.flow.event

import play.api.libs.json._

object Util {

  def parseString(json: JsValue, field: String): Option[String] = {
    (json \ field).validate[String] match {
      case JsError(_) => None
      case JsSuccess(value, _) => Some(value)
    }
  }

  def mustParseString(json: JsValue, field: String): String = {
    parseString(json, field).getOrElse {
      sys.error(s"Json value is missing a field named[$field]: $json")
    }
  }

}
