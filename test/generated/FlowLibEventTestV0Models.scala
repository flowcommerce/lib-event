/**
 * Generated by API Builder - https://www.apibuilder.io
 * Service version: 0.0.1
 * apibuilder 0.14.59 app.apibuilder.io/flow/lib-event-test/0.0.1/play_2_x_json
 */
package io.flow.lib.event.test.v0.models {

  sealed trait TestEvent extends _root_.scala.Product with _root_.scala.Serializable

  /**
   * Defines the valid discriminator values for the type TestEvent
   */
  sealed trait TestEventDiscriminator extends _root_.scala.Product with _root_.scala.Serializable

  object TestEventDiscriminator {

    case object TestObjectUpserted extends TestEventDiscriminator { override def toString = "test_object_upserted" }
    case object TestObjectDeleted extends TestEventDiscriminator { override def toString = "test_object_deleted" }

    final case class UNDEFINED(override val toString: String) extends TestEventDiscriminator

    val all: scala.List[TestEventDiscriminator] = scala.List(TestObjectUpserted, TestObjectDeleted)

    private[this] val byName: Map[String, TestEventDiscriminator] = all.map(x => x.toString.toLowerCase -> x).toMap

    def apply(value: String): TestEventDiscriminator = fromString(value).getOrElse(UNDEFINED(value))

    def fromString(value: String): _root_.scala.Option[TestEventDiscriminator] = byName.get(value.toLowerCase)

  }

  final case class TestObject(
    id: String
  )

  final case class TestObjectDeleted(
    eventId: String,
    timestamp: _root_.org.joda.time.DateTime,
    id: String
  ) extends TestEvent

  final case class TestObjectUpserted(
    eventId: String,
    timestamp: _root_.org.joda.time.DateTime,
    testObject: io.flow.lib.event.test.v0.models.TestObject
  ) extends TestEvent

  /**
   * Provides future compatibility in clients - in the future, when a type is added
   * to the union TestEvent, it will need to be handled in the client code. This
   * implementation will deserialize these future types as an instance of this class.
   * 
   * @param description Information about the type that we received that is undefined in this version of
   *        the client.
   */
  final case class TestEventUndefinedType(
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

    private[v0] implicit val jsonReadsJodaLocalDate = __.read[String].map { str =>
      import org.joda.time.format.ISODateTimeFormat.dateParser
      dateParser.parseLocalDate(str)
    }

    private[v0] implicit val jsonWritesJodaLocalDate = new Writes[org.joda.time.LocalDate] {
      def writes(x: org.joda.time.LocalDate) = {
        import org.joda.time.format.ISODateTimeFormat.date
        val str = date.print(x)
        JsString(str)
      }
    }

    implicit def jsonReadsLibEventTestTestObject: play.api.libs.json.Reads[TestObject] = {
      (__ \ "id").read[String].map { x => new TestObject(id = x) }
    }

    def jsObjectTestObject(obj: io.flow.lib.event.test.v0.models.TestObject): play.api.libs.json.JsObject = {
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
      for {
        eventId <- (__ \ "event_id").read[String]
        timestamp <- (__ \ "timestamp").read[_root_.org.joda.time.DateTime]
        id <- (__ \ "id").read[String]
      } yield TestObjectDeleted(eventId, timestamp, id)
    }

    def jsObjectTestObjectDeleted(obj: io.flow.lib.event.test.v0.models.TestObjectDeleted): play.api.libs.json.JsObject = {
      play.api.libs.json.Json.obj(
        "event_id" -> play.api.libs.json.JsString(obj.eventId),
        "timestamp" -> play.api.libs.json.JsString(_root_.org.joda.time.format.ISODateTimeFormat.dateTime.print(obj.timestamp)),
        "id" -> play.api.libs.json.JsString(obj.id)
      )
    }

    implicit def jsonReadsLibEventTestTestObjectUpserted: play.api.libs.json.Reads[TestObjectUpserted] = {
      for {
        eventId <- (__ \ "event_id").read[String]
        timestamp <- (__ \ "timestamp").read[_root_.org.joda.time.DateTime]
        testObject <- (__ \ "test_object").read[io.flow.lib.event.test.v0.models.TestObject]
      } yield TestObjectUpserted(eventId, timestamp, testObject)
    }

    def jsObjectTestObjectUpserted(obj: io.flow.lib.event.test.v0.models.TestObjectUpserted): play.api.libs.json.JsObject = {
      play.api.libs.json.Json.obj(
        "event_id" -> play.api.libs.json.JsString(obj.eventId),
        "timestamp" -> play.api.libs.json.JsString(_root_.org.joda.time.format.ISODateTimeFormat.dateTime.print(obj.timestamp)),
        "test_object" -> jsObjectTestObject(obj.testObject)
      )
    }

    implicit def jsonReadsLibEventTestTestEvent: play.api.libs.json.Reads[TestEvent] = new play.api.libs.json.Reads[TestEvent] {
      def reads(js: play.api.libs.json.JsValue): play.api.libs.json.JsResult[TestEvent] = {
        (js \ "discriminator").asOpt[String].getOrElse { sys.error("Union[TestEvent] requires a discriminator named 'discriminator' - this field was not found in the Json Value") } match {
          case "test_object_upserted" => js.validate[io.flow.lib.event.test.v0.models.TestObjectUpserted]
          case "test_object_deleted" => js.validate[io.flow.lib.event.test.v0.models.TestObjectDeleted]
          case other => play.api.libs.json.JsSuccess(io.flow.lib.event.test.v0.models.TestEventUndefinedType(other))
        }
      }
    }

    def jsObjectTestEvent(obj: io.flow.lib.event.test.v0.models.TestEvent): play.api.libs.json.JsObject = {
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

    // import models directly for backwards compatibility with prior versions of the generator
    import Core._

    object Core {
      implicit def pathBindableDateTimeIso8601(implicit stringBinder: QueryStringBindable[String]): PathBindable[_root_.org.joda.time.DateTime] = ApibuilderPathBindable(ApibuilderTypes.dateTimeIso8601)
      implicit def queryStringBindableDateTimeIso8601(implicit stringBinder: QueryStringBindable[String]): QueryStringBindable[_root_.org.joda.time.DateTime] = ApibuilderQueryStringBindable(ApibuilderTypes.dateTimeIso8601)

      implicit def pathBindableDateIso8601(implicit stringBinder: QueryStringBindable[String]): PathBindable[_root_.org.joda.time.LocalDate] = ApibuilderPathBindable(ApibuilderTypes.dateIso8601)
      implicit def queryStringBindableDateIso8601(implicit stringBinder: QueryStringBindable[String]): QueryStringBindable[_root_.org.joda.time.LocalDate] = ApibuilderQueryStringBindable(ApibuilderTypes.dateIso8601)
    }

    trait ApibuilderTypeConverter[T] {

      def convert(value: String): T

      def convert(value: T): String

      def example: T

      def validValues: Seq[T] = Nil

      def errorMessage(key: String, value: String, ex: java.lang.Exception): String = {
        val base = s"Invalid value '$value' for parameter '$key'. "
        validValues.toList match {
          case Nil => base + "Ex: " + convert(example)
          case values => base + ". Valid values are: " + values.mkString("'", "', '", "'")
        }
      }
    }

    object ApibuilderTypes {
      import org.joda.time.{format, DateTime, LocalDate}

      val dateTimeIso8601: ApibuilderTypeConverter[DateTime] = new ApibuilderTypeConverter[DateTime] {
        override def convert(value: String): DateTime = format.ISODateTimeFormat.dateTimeParser.parseDateTime(value)
        override def convert(value: DateTime): String = format.ISODateTimeFormat.dateTime.print(value)
        override def example: DateTime = DateTime.now
      }

      val dateIso8601: ApibuilderTypeConverter[LocalDate] = new ApibuilderTypeConverter[LocalDate] {
        override def convert(value: String): LocalDate = format.ISODateTimeFormat.yearMonthDay.parseLocalDate(value)
        override def convert(value: LocalDate): String = value.toString
        override def example: LocalDate = LocalDate.now
      }

    }

    final case class ApibuilderQueryStringBindable[T](
      converters: ApibuilderTypeConverter[T]
    ) extends QueryStringBindable[T] {

      override def bind(key: String, params: Map[String, Seq[String]]): _root_.scala.Option[_root_.scala.Either[String, T]] = {
        params.getOrElse(key, Nil).headOption.map { v =>
          try {
            Right(
              converters.convert(v)
            )
          } catch {
            case ex: java.lang.Exception => Left(
              converters.errorMessage(key, v, ex)
            )
          }
        }
      }

      override def unbind(key: String, value: T): String = {
        s"$key=${converters.convert(value)}"
      }
    }

    final case class ApibuilderPathBindable[T](
      converters: ApibuilderTypeConverter[T]
    ) extends PathBindable[T] {

      override def bind(key: String, value: String): _root_.scala.Either[String, T] = {
        try {
          Right(
            converters.convert(value)
          )
        } catch {
          case ex: java.lang.Exception => Left(
            converters.errorMessage(key, value, ex)
          )
        }
      }

      override def unbind(key: String, value: T): String = {
        converters.convert(value)
      }
    }

  }

}
