/**
 * Generated by apidoc - http://www.apidoc.me
 * Service version: 0.0.1
 * apidoc:0.11.76 http://www.apidoc.me/flow/lib-event-test/0.0.1/play_2_x_json
 */
package io.flow.lib.event.test.v0.models {

  sealed trait TestEvent

  /**
   * Defines the valid discriminator values for the type TestEvent
   */
  sealed trait TestEventDiscriminator

  object TestEventDiscriminator {

    case object TestObjectUpserted extends TestEventDiscriminator { override def toString = "test_object_upserted" }
    case object TestObjectDeleted extends TestEventDiscriminator { override def toString = "test_object_deleted" }

    case class UNDEFINED(override val toString: String) extends TestEventDiscriminator

    val all = Seq(TestObjectUpserted, TestObjectDeleted)

    private[this] val byName = all.map(x => x.toString.toLowerCase -> x).toMap

    def apply(value: String): TestEventDiscriminator = fromString(value).getOrElse(UNDEFINED(value))

    def fromString(value: String): _root_.scala.Option[TestEventDiscriminator] = byName.get(value.toLowerCase)

  }

  case class TestObject(
    id: String
  )

  case class TestObjectDeleted(
    eventId: String,
    timestamp: _root_.org.joda.time.DateTime,
    id: String
  ) extends TestEvent

  case class TestObjectUpserted(
    eventId: String,
    timestamp: _root_.org.joda.time.DateTime,
    testObject: io.flow.lib.event.test.v0.models.TestObject
  ) extends TestEvent

  /**
   * Provides future compatibility in clients - in the future, when a type is added
   * to the union TestEvent, it will need to be handled in the client code. This
   * implementation will deserialize these future types as an instance of this class.
   */
  case class TestEventUndefinedType(
    description: String
  ) extends TestEvent

}

package io.flow.lib.event.test.v0.models {

  package object json {
    import play.api.libs.json.__
    import play.api.libs.json.JsString
    import play.api.libs.json.Writes
    import play.api.libs.functional.syntax._
    import io.flow.lib.event.test.v0.models.json._

    private[v0] implicit val jsonReadsUUID = __.read[String].map(java.util.UUID.fromString)

    private[v0] implicit val jsonWritesUUID = new Writes[java.util.UUID] {
      def writes(x: java.util.UUID) = JsString(x.toString)
    }

    private[v0] implicit val jsonReadsJodaDateTime = __.read[String].map { str =>
      import org.joda.time.format.ISODateTimeFormat.dateTimeParser
      dateTimeParser.parseDateTime(str)
    }

    private[v0] implicit val jsonWritesJodaDateTime = new Writes[org.joda.time.DateTime] {
      def writes(x: org.joda.time.DateTime) = {
        import org.joda.time.format.ISODateTimeFormat.dateTime
        val str = dateTime.print(x)
        JsString(str)
      }
    }

    implicit def jsonReadsLibEventTestTestObject: play.api.libs.json.Reads[TestObject] = {
      (__ \ "id").read[String].map { x => new TestObject(id = x) }
    }

    def jsObjectTestObject(obj: io.flow.lib.event.test.v0.models.TestObject) = {
      play.api.libs.json.Json.obj(
        "id" -> play.api.libs.json.JsString(obj.id)
      )
    }

    implicit def jsonWritesLibEventTestTestObject: play.api.libs.json.Writes[TestObject] = {
      new play.api.libs.json.Writes[io.flow.lib.event.test.v0.models.TestObject] {
        def writes(obj: io.flow.lib.event.test.v0.models.TestObject) = {
          jsObjectTestObject(obj)
        }
      }
    }

    implicit def jsonReadsLibEventTestTestObjectDeleted: play.api.libs.json.Reads[TestObjectDeleted] = {
      (
        (__ \ "event_id").read[String] and
        (__ \ "timestamp").read[_root_.org.joda.time.DateTime] and
        (__ \ "id").read[String]
      )(TestObjectDeleted.apply _)
    }

    def jsObjectTestObjectDeleted(obj: io.flow.lib.event.test.v0.models.TestObjectDeleted) = {
      play.api.libs.json.Json.obj(
        "event_id" -> play.api.libs.json.JsString(obj.eventId),
        "timestamp" -> play.api.libs.json.JsString(_root_.org.joda.time.format.ISODateTimeFormat.dateTime.print(obj.timestamp)),
        "id" -> play.api.libs.json.JsString(obj.id)
      )
    }

    implicit def jsonReadsLibEventTestTestObjectUpserted: play.api.libs.json.Reads[TestObjectUpserted] = {
      (
        (__ \ "event_id").read[String] and
        (__ \ "timestamp").read[_root_.org.joda.time.DateTime] and
        (__ \ "test_object").read[io.flow.lib.event.test.v0.models.TestObject]
      )(TestObjectUpserted.apply _)
    }

    def jsObjectTestObjectUpserted(obj: io.flow.lib.event.test.v0.models.TestObjectUpserted) = {
      play.api.libs.json.Json.obj(
        "event_id" -> play.api.libs.json.JsString(obj.eventId),
        "timestamp" -> play.api.libs.json.JsString(_root_.org.joda.time.format.ISODateTimeFormat.dateTime.print(obj.timestamp)),
        "test_object" -> jsObjectTestObject(obj.testObject)
      )
    }

    implicit def jsonReadsLibEventTestTestEvent: play.api.libs.json.Reads[TestEvent] = new play.api.libs.json.Reads[TestEvent] {
      def reads(js: play.api.libs.json.JsValue): play.api.libs.json.JsResult[TestEvent] = {
        (js \ "discriminator").validate[String] match {
          case play.api.libs.json.JsError(msg) => play.api.libs.json.JsError(msg)
          case play.api.libs.json.JsSuccess(discriminator, _) => {
            discriminator match {
              case "test_object_upserted" => js.validate[io.flow.lib.event.test.v0.models.TestObjectUpserted]
              case "test_object_deleted" => js.validate[io.flow.lib.event.test.v0.models.TestObjectDeleted]
              case other => play.api.libs.json.JsSuccess(io.flow.lib.event.test.v0.models.TestEventUndefinedType(other))
            }
          }
        }
      }
    }

    def jsObjectTestEvent(obj: io.flow.lib.event.test.v0.models.TestEvent) = {
      obj match {
        case x: io.flow.lib.event.test.v0.models.TestObjectUpserted => jsObjectTestObjectUpserted(x) ++ play.api.libs.json.Json.obj("discriminator" -> "test_object_upserted")
        case x: io.flow.lib.event.test.v0.models.TestObjectDeleted => jsObjectTestObjectDeleted(x) ++ play.api.libs.json.Json.obj("discriminator" -> "test_object_deleted")
        case other => {
          sys.error(s"The type[${other.getClass.getName}] has no JSON writer")
        }
      }
    }

    implicit def jsonWritesLibEventTestTestEvent: play.api.libs.json.Writes[TestEvent] = {
      new play.api.libs.json.Writes[io.flow.lib.event.test.v0.models.TestEvent] {
        def writes(obj: io.flow.lib.event.test.v0.models.TestEvent) = {
          jsObjectTestEvent(obj)
        }
      }
    }
  }
}

package io.flow.lib.event.test.v0 {

  object Bindables {

    import play.api.mvc.{PathBindable, QueryStringBindable}
    import org.joda.time.{DateTime, LocalDate}
    import org.joda.time.format.ISODateTimeFormat
    import io.flow.lib.event.test.v0.models._

    // Type: date-time-iso8601
    implicit val pathBindableTypeDateTimeIso8601 = new PathBindable.Parsing[org.joda.time.DateTime](
      ISODateTimeFormat.dateTimeParser.parseDateTime(_), _.toString, (key: String, e: _root_.java.lang.Exception) => s"Error parsing date time $key. Example: 2014-04-29T11:56:52Z"
    )

    implicit val queryStringBindableTypeDateTimeIso8601 = new QueryStringBindable.Parsing[org.joda.time.DateTime](
      ISODateTimeFormat.dateTimeParser.parseDateTime(_), _.toString, (key: String, e: _root_.java.lang.Exception) => s"Error parsing date time $key. Example: 2014-04-29T11:56:52Z"
    )

    // Type: date-iso8601
    implicit val pathBindableTypeDateIso8601 = new PathBindable.Parsing[org.joda.time.LocalDate](
      ISODateTimeFormat.yearMonthDay.parseLocalDate(_), _.toString, (key: String, e: _root_.java.lang.Exception) => s"Error parsing date $key. Example: 2014-04-29"
    )

    implicit val queryStringBindableTypeDateIso8601 = new QueryStringBindable.Parsing[org.joda.time.LocalDate](
      ISODateTimeFormat.yearMonthDay.parseLocalDate(_), _.toString, (key: String, e: _root_.java.lang.Exception) => s"Error parsing date $key. Example: 2014-04-29"
    )



  }

}