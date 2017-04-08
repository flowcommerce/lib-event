package io.flow.event.v2

import java.util.UUID

import io.flow.event.Record
import io.flow.lib.event.test.v0.models.{TestEvent, TestObject, TestObjectUpserted}
import io.flow.lib.event.test.v0.models.json._
import io.flow.play.clients.MockConfig
import io.flow.play.util.{Config, IdGenerator}
import org.joda.time.DateTime
import org.scalatestplus.play.{OneAppPerSuite, PlaySpec}
import play.api.libs.json.Json
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Seconds, Span}

class QueueSpec extends PlaySpec with OneAppPerSuite {

  import scala.concurrent.ExecutionContext.Implicits.global

  private[this] lazy val config = play.api.Play.current.injector.instanceOf[MockConfig]

  private[this] val eventIdGenerator = IdGenerator("evt")

  def eventuallyInNSeconds[T](n: Int)(f: => T): T = {
    eventually(timeout(Span(n, Seconds))) {
      f
    }
  }

  def withConfig[T](f: Config => T): T = {
    config.set("name", "lib-event-test")
    f(config)
  }

  def publish(producer: Producer, o: TestObject): String = {
    val eventId = eventIdGenerator.randomId()
    producer.publish(
      Json.toJson(
        TestObjectUpserted(
          eventId = eventId,
          timestamp = DateTime.now,
          testObject = o
        )
      )
    )
    eventId
  }

  def consume(consumer: Consumer, eventId: String, timeoutSeconds: Int = 30): Record = {
    var selectedEvent: Option[Record] = None
    eventuallyInNSeconds(timeoutSeconds) {
      consumer.consume { rec =>
        if (rec.eventId == eventId) {
          selectedEvent = Some(rec)
        }
      }
      selectedEvent.get
    }
  }

  "can publish and consume an event" in {
    withConfig { config =>
      val testObject = TestObject(UUID.randomUUID().toString)

      val q = new DefaultQueue(config)
      val producer = q.producer[TestEvent]()

      val eventId = publish(producer, testObject)
      println(s"Published event[$eventId]")

      val consumer = q.consumer[TestEvent]
      val fetched = consume(consumer, eventId)
      println(s"fetched: $fetched")
      fetched.js.as[TestObjectUpserted].testObject.id must equal(testObject.id)
    }
  }

}
