package io.flow.event.v2

import io.flow.event.Record
import io.flow.lib.event.test.v0.models.json._
import io.flow.lib.event.test.v0.models.{TestEvent, TestObject, TestObjectUpserted}
import io.flow.log.RollbarLogger
import io.flow.play.clients.MockConfig
import io.flow.play.metrics.MockMetricsSystem
import io.flow.util.IdGenerator
import org.joda.time.DateTime
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Seconds, Span}
import play.api.Application

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.FiniteDuration
import scala.reflect.runtime.universe._

trait Helpers {

  import scala.concurrent.ExecutionContext.Implicits.global

  private[this] def config(implicit app: Application) = app.injector.instanceOf[MockConfig]

  val eventIdGenerator = IdGenerator("evt")

  def eventuallyInNSeconds[T](n: Int)(f: => T): T = {
    eventually(timeout(Span(n.toLong, Seconds))) {
      f
    }
  }

  def withConfig[T](f: MockConfig => T)(implicit app: Application): T = {
    val c = config
    c.set("name", "lib-event-test")
    f(c)
  }

  def withQueue[T](f: DefaultQueue => T)(implicit app: Application): T = {
    withConfig { config =>
      val creds = new AWSCreds(config)
      val endpoints = app.injector.instanceOf[AWSEndpoints]
      val metrics = new MockMetricsSystem()
      val rollbar = RollbarLogger.SimpleLogger

      f(new DefaultQueue(config, creds, endpoints, metrics, rollbar))
    }
  }

  def publishTestObject(producer: Producer[TestEvent], o: TestObject): String = {
    val eventId = eventIdGenerator.randomId()
    producer.publish(
      TestObjectUpserted(
        eventId = eventId,
        timestamp = DateTime.now,
        testObject = o
      )
    )
    eventId
  }

  def consume[T: TypeTag](q: Queue, eventId: String, timeoutSeconds: Int = 120): Record = {
    consumeUntil[T](q, eventId, timeoutSeconds).find(_.eventId == eventId).getOrElse {
      sys.error(s"Failed to find eventId[$eventId]")
    }
  }

  def consumeUntil[T: TypeTag](q: Queue, eventId: String, timeoutSeconds: Int = 120): Seq[Record] = {
    val all = scala.collection.mutable.ListBuffer[Record]()
    q.consume[T] { recs =>
      all ++= recs
    }

    Await.result(
      Future {
        while (!all.exists(_.eventId == eventId)) {
          Thread.sleep(100)
        }
      },
      FiniteDuration(timeoutSeconds.toLong, "seconds")
    )

    q.shutdown

    all
  }
}

