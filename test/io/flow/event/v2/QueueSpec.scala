package io.flow.event.v2

import io.flow.lib.event.test.v0.models.{TestEvent, TestObject, TestObjectUpserted}
import io.flow.lib.event.test.v0.models.json._
import java.util.UUID
import org.scalatestplus.play.{OneAppPerSuite, PlaySpec}

class QueueSpec extends PlaySpec with OneAppPerSuite with Helpers {

  import scala.concurrent.ExecutionContext.Implicits.global

  /*
  "can publish and consume an event" in {
    withConfig { config =>
      val testObject = TestObject(UUID.randomUUID().toString)

      val q = new DefaultQueue(config)
      val producer = q.producer[TestEvent]()

      val eventId = publishTestObject(producer, testObject)
      println(s"Published event[$eventId]. Waiting for consumer")

      val consumer = q.consumer[TestEvent]
      val fetched = consume(consumer, eventId)
      fetched.js.as[TestObjectUpserted].testObject.id must equal(testObject.id)

      consumer.shutdown
      producer.shutdown
    }
  }
*/

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
