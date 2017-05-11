package io.flow.event

import play.api.libs.json._

import scala.util.{Failure, Success, Try}

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

  def getJsValue(json: JsValue, field: String): JsValue = {
    (json \ field).validate[JsValue] match {
      case JsError(_) => sys.error(s"Json value is missing a field named[$field]: $json")
      case JsSuccess(value, _) => value
    }
  }

  def validateLong(js: JsValue): Either[String, Long] = {
    js match {
      case v: JsArray => {
        v.value.size match {
          case 1 => validateLong(v.value.head)
          case _ => Left(s"$v must be a long and not an array")
        }
      }
      case v: JsBoolean => Left(s"$v must be a long and not a boolean")
      case JsNull => Left(s"$js must be a long and not null")
      case v: JsNumber => v.asOpt[Long] match {
        case None => Left(s"$v must be a valid long")
        case Some(n) => Right(n)
      }
      case v: JsObject => Left(s"$v must be a long and not an object")
      case v: JsString => {
        Try {
          v.value.toLong
        } match {
          case Success(v) => Right(v)
          case Failure(_) => Left(s"$js must be a valid long")
        }
      }
    }

  }

  def mustParseLong(json: JsValue, field: String): Long = {
    validateLong(getJsValue(json, field)) match {
      case Left(errors) => sys.error { s"Could not parse field named[$field] as a long: $errors" }
      case Right(l) => l
    }
  }

}
