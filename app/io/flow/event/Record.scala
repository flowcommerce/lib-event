package io.flow.event

import play.api.libs.json.{JsValue, Json}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat.dateTimeParser
import scala.reflect.runtime.universe._

object Record {

  def fromByteArray(arrivalTimestamp: DateTime, value: Array[Byte]): Record = {
    fromJsValue(arrivalTimestamp, Json.parse(value))
  }

  def fromJsValue(arrivalTimestamp: DateTime, js: JsValue): Record = {
    Record(
      eventId = Util.mustParseString(js, "event_id"),
      timestamp = dateTimeParser.parseDateTime(
        Util.mustParseString(js, "timestamp")
      ),
      js = js,
      arrivalTimestamp = arrivalTimestamp
    )
  }
  
}

case class Record(
  eventId: String,
  timestamp: DateTime,
  arrivalTimestamp: DateTime,
  js: JsValue
){
  /** Returns the APIBuilder discriminator of the event */
  lazy val discriminator = (js \ "discriminator").asOpt[String]
}

case class Message(
  message: String,
  arrivalTimestamp: DateTime
)
