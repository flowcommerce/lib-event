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
    eventType = DynamoStreamEventType(record.getEventName),
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
  val eventType: DynamoStreamEventType,
  val newImage: Option[java.util.Map[String, AttributeValue]],
  val oldImage: Option[java.util.Map[String, AttributeValue]]
) extends Record(eventId, timestamp, arrivalTimestamp, js) {
  override lazy val discriminator: Option[String] = Some(recordType.typeSymbol.fullName)

  override def toString: String = s"DynamoStreamRecord($eventId,$timestamp,$arrivalTimestamp,$js,$recordType,$eventType,$newImage,$oldImage,$discriminator)"
}

sealed trait DynamoStreamEventType
object DynamoStreamEventType {
  case object Insert extends DynamoStreamEventType { override def toString = "INSERT" }
  case object Modify extends DynamoStreamEventType { override def toString = "MODIFY" }
  case object Remove extends DynamoStreamEventType { override def toString = "REMOVE" }
  final case class UNDEFINED(override val toString: String) extends DynamoStreamEventType

  val all = scala.List(Insert, Modify, Remove)
  private[this] val byName = all.map(x => x.toString.toLowerCase -> x).toMap

  def apply(value: String): DynamoStreamEventType = fromString(value).getOrElse(UNDEFINED(value))
  def fromString(value: String): Option[DynamoStreamEventType] = byName.get(value.toLowerCase)
}


