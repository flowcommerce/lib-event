package io.flow.event

import io.flow.play.util.Booleans
import play.api.Logger
import play.api.libs.json._

import scala.util.{Failure, Success, Try}

/**
  * Helper to parse values of a specific type from a JsValue handling common
  * translations (e.g. "123" and "123" can both be successfully parsed as an Int)
  */
object JsonUtil {

  def requiredString(json: JsValue, field: String): String = mustGet(json, field, optionalString(json, field))

  def optionalString(json: JsValue, field: String): Option[String] = {
    (json \ field).validate[JsValue] match {
      case JsError(_) => sys.error(s"Field[$field] is required")
      case JsSuccess(value, _) => {
        val stringValue = value match {
          case v: JsBoolean => v.value.toString()
          case v: JsNumber => v.value.toString()
          case v: JsString => v.value
          case _ => ""
        }
        stringValue.trim match {
          case "" => None
          case v => Some(v)
        }
      }
    }
  }

  def requiredPositiveLong(json: JsValue, field: String): Long = mustGet(json, field, optionalPositiveLong(json, field))

  def optionalPositiveLong(json: JsValue, field: String): Option[Long] = optionalLong(json, field) match {
    case None => None
    case Some(v) => if (v > 0) {
      Some(v)
    } else {
      sys.error(s"Field[$field] has invalid value[$v]: must be > 0")
    }
  }

  def requiredLong(json: JsValue, field: String): Long = mustGet(json, field, optionalLong(json, field))

  def optionalLong(json: JsValue, field: String): Option[Long] = optionalString(json, field).map { value =>
    Try(value.toLong) match {
      case Success(v) => v
      case Failure(_) => {
        val msg = s"Field[$field] has invalid value[$value]: must be a long"
        Logger.error(msg)
        sys.error(msg)
      }
    }
  }


  def requiredPositiveInt(json: JsValue, field: String): Int = mustGet(json, field, optionalPositiveInt(json, field))

  def optionalPositiveInt(json: JsValue, field: String): Option[Int] = optionalInt(json, field) match {
    case None => None
    case Some(v) => if (v > 0) {
      Some(v)
    } else {
      sys.error(s"Field[$field] has invalid value[$v]: must be > 0")
    }
  }

  def requiredInt(json: JsValue, field: String): Int = mustGet(json, field, optionalInt(json, field))

  def optionalInt(json: JsValue, field: String): Option[Int] = optionalString(json, field).map { value =>
    Try(value.toInt) match {
      case Success(v) => v
      case Failure(_) => {
        val msg = s"Field[$field] has invalid value[$value]: must be an int"
        Logger.error(msg)
        sys.error(msg)
      }
    }
  }

  def requiredBoolean(json: JsValue, field: String): Boolean = mustGet(json, field, optionalBoolean(json, field))

  def optionalBoolean(json: JsValue, field: String): Option[Boolean] = optionalString(json, field).map { value =>
    Booleans.parse(value).getOrElse {
      val msg = s"Field[$field] has invalid value[$value]. Use true, t, false, or f"
      Logger.error(msg)
      sys.error(msg)
    }
  }

  private[this] def mustGet[T](json: JsValue, field: String, value: Option[T]): T = {
    value.getOrElse {
      (json \ field).validate[JsValue] match {
        case JsError(_) => sys.error(s"Field[$field] is required")
        case JsSuccess(_, _) => sys.error(s"Field[$field] cannot be empty")
      }
    }
  }

}