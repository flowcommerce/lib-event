package io.flow.event.v2

import io.flow.lib.event.test.v0.models.{TestEvent, TestObject, TestObjectUpserted}
import io.flow.lib.event.test.v0.models.json._
import io.flow.play.clients.MockConfig
import io.flow.play.util.Config
import org.joda.time.DateTime
import org.scalatestplus.play.{OneAppPerSuite, PlaySpec}
import play.api.libs.json.Json

class QueueSpec extends PlaySpec with OneAppPerSuite {

  import scala.concurrent.ExecutionContext.Implicits.global

  private[this] lazy val config = play.api.Play.current.injector.instanceOf[MockConfig]

  def withConfig[T](f: Config => T): T = {
    config.set("name", "lib-event-test")
    f(config)
  }

  "can publish and consume an event" in {
    withConfig { config =>
      val q = new DefaultQueue(config)

      val producer = q.producer[TestEvent]()
      val consumer = q.consumer[TestEvent]

      println("Publishing event")
      producer.publish(
        Json.toJson(
          TestObjectUpserted(
            eventId = "1",
            timestamp = DateTime.now,
            testObject = TestObject("obj-1")
          )
        )
      )

      println("Published. Waiting to consume event")

      Thread.sleep(1500)
      consumer.consume { js =>
        println(s"Consumed js: $js")
      }

      Thread.sleep(1500)
      consumer.consume { js =>
        println(s"Consumed js: $js")
      }

      Thread.sleep(1500)
      consumer.consume { js =>
        println(s"Consumed js: $js")
      }

      producer.shutdown
      consumer.shutdown
      println("Published. Done consuming event")
    }
  }

}
