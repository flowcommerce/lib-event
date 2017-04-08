package io.flow.event.v2

import io.flow.event.Record
import io.flow.lib.event.test.v0.models.{TestObject, TestObjectUpserted}
import org.scalatestplus.play.{OneAppPerSuite, PlaySpec}

class MockQueueSpec extends PlaySpec with OneAppPerSuite with Helpers {

  private[this] val testObject = TestObject(id = "1")

  "all" in {
    val q = new MockQueue()
    val producer = q.producer[TestObjectUpserted]()
    val consumer = q.consumer[TestObjectUpserted].asInstanceOf[MockConsumer]

    consumer.consume { _ =>
      sys.error("Queue should be empty")
    }

    val eventId = publishTestObject(producer, testObject)
    var rec: Option[Record] = None
    consumer.consume { r =>
      rec = Some(r)
    }
    rec.get.eventId must equal(eventId)

    consumer.stream.all.map(_.eventId) must equal(Seq(eventId))
    consumer.stream.consumed.map(_.eventId) must equal(Seq(eventId))
    consumer.stream.pending.map(_.eventId) must equal(Nil)
  }

  "consumeEventId" in {
    val q = new MockQueue()
    val producer = q.producer[TestObjectUpserted]()
    val consumer = q.consumer[TestObjectUpserted].asInstanceOf[MockConsumer]

    val eventId = publishTestObject(producer, testObject)
    consumer.consumeEventId(eventId).get.eventId must equal(eventId)
  }

}
