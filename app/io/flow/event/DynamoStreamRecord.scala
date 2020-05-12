package io.flow.event

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import org.joda.time.DateTime
import play.api.libs.json.{JsNull, JsValue}

import scala.reflect.runtime.universe._

object DynamoStreamRecord {
  def apply(recordType: Type, record: com.amazonaws.services.dynamodbv2.model.Record) = new DynamoStreamRecord(
    eventId = record.getEventID,
    timestamp = DateTime.now,
    arrivalTimestamp = new DateTime(record.getDynamodb.getApproximateCreationDateTime),
    recordType = recordType,
    js = JsNull,
    eventName = DynamoStreamEventName(record.getEventName),
    newImage = Option(record.getDynamodb.getNewImage),
    oldImage = Option(record.getDynamodb.getOldImage)
  )
}

class DynamoStreamRecord(
  override val eventId: String,
  override val timestamp: DateTime,
  override val arrivalTimestamp: DateTime,
  override val js: JsValue,
  val recordType: Type,
  val eventName: DynamoStreamEventName,
  val newImage: Option[java.util.Map[String, AttributeValue]],
  val oldImage: Option[java.util.Map[String, AttributeValue]]
) extends Record(eventId, timestamp, arrivalTimestamp, js) {
  override lazy val discriminator: Option[String] = Some(recordType.typeSymbol.name.toString)
}

sealed trait DynamoStreamEventName
object DynamoStreamEventName {
  case object Insert extends DynamoStreamEventName { override def toString = "INSERT" }
  case object Modify extends DynamoStreamEventName { override def toString = "MODIFY" }
  case object Remove extends DynamoStreamEventName { override def toString = "REMOVE" }
  final case class UNDEFINED(override val toString: String) extends DynamoStreamEventName

  val all = scala.List(Insert, Modify, Remove)
  private[this] val byName = all.map(x => x.toString.toLowerCase -> x).toMap

  def apply(value: String): DynamoStreamEventName = fromString(value).getOrElse(UNDEFINED(value))
  def fromString(value: String): Option[DynamoStreamEventName] = byName.get(value.toLowerCase)
}


