package io.flow.event.v2

import io.flow.lib.event.test.v0.models.{TestEvent, TestObject, TestObjectUpserted}
import io.flow.lib.event.test.v0.models.json._
import java.util.UUID
import org.scalatestplus.play.{OneAppPerSuite, PlaySpec}

class QueueSpec extends PlaySpec with OneAppPerSuite with Helpers {

  import scala.concurrent.ExecutionContext.Implicits.global

  "can publish and consume an event" in {
    withConfig { config =>
      val testObject = TestObject(UUID.randomUUID().toString)

      val q = new DefaultQueue(config)
      val producer = q.producer[TestEvent]()

      val eventId = publishTestObject(producer, testObject)
      println(s"Published event[$eventId]. Waiting for consumer")

      val fetched = consume[TestEvent](q, eventId)
      fetched.js.as[TestObjectUpserted].testObject.id must equal(testObject.id)

      q.shutdown
    }
  }
/*
  "keeps track of sequence number" in {
    withConfig { config =>
      val testObject1 = TestObject(UUID.randomUUID().toString)

      val q = new DefaultQueue(config)
      val producer = q.producer[TestEvent]()

      val eventId1 = publishTestObject(producer, testObject1)
      println(s"Published event[$eventId1]. Waiting for consumer1")

      consume(q, eventId1)

      // Now create a second consumer and verify it does not see testObject1
      val testObject2 = TestObject(UUID.randomUUID().toString)
      val eventId2 = publishTestObject(producer, testObject2)
      val testObject3 = TestObject(UUID.randomUUID().toString)
      val eventId3 = publishTestObject(producer, testObject3)

      val all = consumeUntil[TestEvent](q, eventId3)
      all.map(_.eventId) must equal(Seq(eventId2, eventId3))

      q.shutdown
    }
  }
*/
}
