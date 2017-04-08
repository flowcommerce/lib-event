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

  def publishTestObject(producer: Producer, o: TestObject): String = {
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

  def consume(consumer: Consumer, eventId: String, timeoutSeconds: Int = 60): Record = {
    consumeUntil(consumer, eventId, timeoutSeconds).find(_.eventId == eventId).getOrElse {
      sys.error(s"Failed to find eventId[$eventId]")
    }
  }

  def consumeUntil(consumer: Consumer, eventId: String, timeoutSeconds: Int = 60): Seq[Record] = {
    var selectedEvent: Option[Record] = None
    val all = scala.collection.mutable.ListBuffer[Record]()

    eventuallyInNSeconds(timeoutSeconds) {
      consumer.consume { rec =>
        all.append(rec)
        if (rec.eventId == eventId) {
          selectedEvent = Some(rec)
        }
      }
      selectedEvent.get
    }

    all
  }

  "can publish and consume an event" in {
    withConfig { config =>
      val testObject = TestObject(UUID.randomUUID().toString)

      val q = new DefaultQueue(config)
      val producer = q.producer[TestEvent]()

      val eventId = publishTestObject(producer, testObject)
      println(s"Published event[$eventId]. Waiting for consumer")

      val consumer = q.consumer[TestEvent]
      val fetched = consume(consumer, eventId)
      println(s"fetched: $fetched")
      fetched.js.as[TestObjectUpserted].testObject.id must equal(testObject.id)
    }
  }

  "keeps track of sequence number" in {
    withConfig { config =>
      val testObject1 = TestObject(UUID.randomUUID().toString)

      val q = new DefaultQueue(config)
      val producer = q.producer[TestEvent]()

      val eventId1 = publishTestObject(producer, testObject1)
      println(s"Published event[$eventId1]. Waiting for consumer1")

      val consumer1 = q.consumer[TestEvent]
      consume(consumer1, eventId1)
      consumer1.shutdown

      // Now create a second consumer and verify it does not see testObject1
      val testObject2 = TestObject(UUID.randomUUID().toString)
      val eventId2 = publishTestObject(producer, testObject2)
      val testObject3 = TestObject(UUID.randomUUID().toString)
      val eventId3 = publishTestObject(producer, testObject3)

      val consumer2 = q.consumer[TestEvent]
      val all = consumeUntil(consumer2, eventId3)
      all.map(_.eventId) must equal(Seq(eventId2, eventId3))

      consumer2.shutdown
      producer.shutdown
    }
  }

}
