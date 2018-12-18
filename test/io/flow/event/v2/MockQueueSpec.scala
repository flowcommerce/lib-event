package io.flow.event.v2

import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicReference, LongAdder}

import io.flow.event.Record
import io.flow.lib.event.test.v0.models.{TestEvent, TestObject}
import io.flow.log.RollbarLogger
import org.scalatest.mockito.MockitoSugar
import org.scalatestplus.play.guice.GuiceOneAppPerSuite
import org.scalatestplus.play.PlaySpec

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

class MockQueueSpec extends PlaySpec with GuiceOneAppPerSuite with Helpers with MockitoSugar {

  private[this] val testObject = TestObject(id = "1")
  private[this] val logger = new RollbarLogger(rollbar = None, attributes = Map.empty, legacyMessage = None)

  "all" in {
    val q = new MockQueue(logger)
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

    StreamUsage.writtenStreams.head.eventsProduced.size must be(1)
  }

  "consumeEventId" in {
    val q = new MockQueue(logger)
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

    val q = new MockQueue(logger)
    val producer = q.producer[TestEvent]()

    val count = new LongAdder()

    // consume: start [[consumersSize]] consumers consuming concurrently
    (1 to consumersPoolSize).foreach { _ =>
      Future {
        q.consume[TestEvent](_ => count.increment(), 1.nano)
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

  "shutdown consumers" in {
    val q = new MockQueue(logger)

    // let's make sure the stream is empty
    q.stream[TestEvent].pending mustBe empty

    // produce an element every 3 ms
    val producer = q.producer[TestEvent]()
    val producerRunnable = new Runnable {
      override def run(): Unit = publishTestObject(producer, testObject)
    }
    Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(producerRunnable, 0, 100, TimeUnit.MILLISECONDS)

    // eventually stream should contain pending elements
    eventuallyInNSeconds(2) {
      q.stream[TestEvent].pending.size must be > 1
    }

    // consume every 1 ms
    q.consume[TestEvent](_ => (), 1.milli)

    // eventually, stream should be almost empty
    eventuallyInNSeconds(5) {
      q.stream[TestEvent].pending.size must be <= 1
    }

    q.shutdownConsumers

    // eventually stream should not be almost empty any more
    eventuallyInNSeconds(2) {
      q.stream[TestEvent].pending.size must be > 1
    }

    // bring in 2 consumers on the same stream
    q.consume[TestEvent](_ => (), 1.milli)
    q.consume[TestEvent](_ => (), 1.milli)

    eventuallyInNSeconds(5) {
      q.stream[TestEvent].pending.size must be <= 1
    }

    q.shutdownConsumers

    // eventually stream should not be almost empty any more
    eventuallyInNSeconds(2) {
      q.stream[TestEvent].pending.size must be > 1
    }
  }

  "clear the queue" in {
    val q = new MockQueue(logger)

    val producer = q.producer[TestEvent]()
    1 to 10 foreach { _ => publishTestObject(producer, testObject) }

    q.stream[TestEvent].pending must have size 10

    q.clear()
    q.stream[TestEvent].pending mustBe empty
  }

}
