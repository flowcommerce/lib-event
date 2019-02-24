package io.flow.event

import play.api.libs.json.{JsValue, Json}
import java.time.Instant
import java.time.format.DateTimeFormatter

object Record {

  def fromByteArray(arrivalTimestamp: Instant, value: Array[Byte]): Record = {
    fromJsValue(arrivalTimestamp, Json.parse(value))
  }

  def fromJsValue(arrivalTimestamp: Instant, js: JsValue): Record = {
    Record(
      eventId = Util.mustParseString(js, "event_id"),
      timestamp = DateTimeFormatter.ISO_INSTANT.parse(
        Util.mustParseString(js, "timestamp"), Instant.from(_)
      ),
      js = js,
      arrivalTimestamp = arrivalTimestamp
    )
  }
  
}

case class Record(
  eventId: String,
  timestamp: Instant,
  arrivalTimestamp: Instant,
  js: JsValue
){
  /** Returns the APIBuilder discriminator of the event */
  lazy val discriminator = (js \ "discriminator").asOpt[String]
}

case class Message(
  message: String,
  arrivalTimestamp: Instant
)
