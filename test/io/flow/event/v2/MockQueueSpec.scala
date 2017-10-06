package io.flow.event.v2

import java.util.concurrent.Executors
import java.util.concurrent.atomic.{AtomicReference, LongAdder}

import io.flow.event.Record
import io.flow.lib.event.test.v0.models.{TestEvent, TestObject}
import org.scalatestplus.play.{OneAppPerSuite, PlaySpec}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

class MockQueueSpec extends PlaySpec with OneAppPerSuite with Helpers {

  import scala.concurrent.ExecutionContext.Implicits.global

  private[this] val testObject = TestObject(id = "1")

  "all" in {
    val q = new MockQueue()
    val producer = q.producer[TestEvent]()
    val stream = q.stream[TestEvent]

    val rec = new AtomicReference[Option[Record]](None)
    q.consume[TestEvent] { r =>
      rec.set(r.headOption)
    }

    val eventId = publishTestObject(producer, testObject)

    eventuallyInNSeconds(1) {
      rec.get.get.eventId mustBe eventId
    }

    stream.all.map(_.eventId) must equal(Seq(eventId))
    stream.consumed.map(_.eventId) must equal(Seq(eventId))
    stream.pending.map(_.eventId) must equal(Nil)
  }

  "consumeEventId" in {
    val q = new MockQueue()
    val producer = q.producer[TestEvent]()

    val eventId = publishTestObject(producer, testObject)
    q.stream[TestEvent].findByEventId(eventId).get.eventId must equal(eventId)
  }

  "produce and consume concurrently" in {
    val consumersPoolSize = 12
    val producersPoolSize = 24
    // the json conversion when publishing is quite heavy and therefore makes it hard to huge a much bigger number
    val eventsSize = 50000
    val producerContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(producersPoolSize))
    val consumerContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(consumersPoolSize))

    val q = new MockQueue()
    val producer = q.producer[TestEvent]()

    val count = new LongAdder()

    // consume: start [[consumersSize]] consumers consuming concurrently
    (1 to consumersPoolSize).foreach { _ =>
      Future {
        q.consume[TestEvent](_ => count.increment(), 1 nano)
      } (consumerContext)
    }

    // publish concurrently
    (1 to eventsSize).foreach { _ =>
      Future {
        publishTestObject(producer, testObject)
      } (producerContext)
    }

    // eventually we should have consumed it all
    eventuallyInNSeconds(10) {
      count.longValue() mustBe eventsSize
    }
  }

  "clear the queue" in {
    val q = new MockQueue()

    val producer = q.producer[TestEvent]()
    1 to 10 foreach { _ => publishTestObject(producer, testObject) }

    q.clear()
    q.stream[TestEvent].pending mustBe empty
  }

}
