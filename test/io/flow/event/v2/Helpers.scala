package io.flow.event.v2

import io.flow.event.Record
import io.flow.lib.event.test.v0.models.json._
import io.flow.lib.event.test.v0.models.{TestObject, TestObjectUpserted}
import io.flow.play.clients.MockConfig
import io.flow.play.util.{Config, IdGenerator}
import org.joda.time.DateTime
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.{Seconds, Span}
import play.api.libs.json.Json

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.FiniteDuration
import scala.reflect.runtime.universe._

trait Helpers {

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

  def consume[T: TypeTag](q: Queue, eventId: String, timeoutSeconds: Int = 35): Record = {
    consumeUntil[T](q, eventId, timeoutSeconds).find(_.eventId == eventId).getOrElse {
      sys.error(s"Failed to find eventId[$eventId]")
    }
  }

  def consumeUntil[T: TypeTag](q: Queue, eventId: String, timeoutSeconds: Int = 35): Seq[Record] = {
    val all = scala.collection.mutable.ListBuffer[Record]()
    // println(s"  --> consumeUntil for eventId[$eventId]")
    q.consume[T] { rec =>
      // println(s"  --> record eventId[${rec.eventId}]")
      all.append(rec)
    }

    Await.result(
      Future {
        while (!all.exists(_.eventId == eventId)) {
          Thread.sleep(100)
        }
      },
      FiniteDuration(timeoutSeconds, "seconds")
    )

    q.shutdown

    all
  }
}

