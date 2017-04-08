package io.flow.event.v2

import io.flow.event.Record
import io.flow.lib.event.test.v0.models.{TestEvent, TestObject, TestObjectUpserted}
import org.scalatestplus.play.{OneAppPerSuite, PlaySpec}

class MockQueueSpec extends PlaySpec with OneAppPerSuite with Helpers {

  import scala.concurrent.ExecutionContext.Implicits.global

  private[this] val testObject = TestObject(id = "1")

  "all" in {
    val q = new MockQueue()
    val producer = q.producer[TestEvent]()
    val stream = q.stream[TestEvent]

    q.consume[TestEvent] { _ =>
      sys.error("Queue should be empty")
    }

    val eventId = publishTestObject(producer, testObject)
    var rec: Option[Record] = None
    q.consume[TestEvent] { r =>
      rec = Some(r)
    }
    rec.get.eventId must equal(eventId)

    stream.all.map(_.eventId) must equal(Seq(eventId))
    stream.consumed.map(_.eventId) must equal(Seq(eventId))
    stream.pending.map(_.eventId) must equal(Nil)
  }

  "consumeEventId" in {
    val q = new MockQueue()
    val producer = q.producer[TestEvent]()

    val eventId = publishTestObject(producer, testObject)
    q.stream[TestEvent].consumeEventId(eventId).get.eventId must equal(eventId)
  }

}
