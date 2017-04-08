package io.flow.event.v2

import io.flow.play.clients.MockConfig
import org.scalatestplus.play.{OneAppPerSuite, PlaySpec}

class ConsumerSpec extends PlaySpec with OneAppPerSuite {

  private[this] lazy val config = play.api.Play.current.injector.instanceOf[MockConfig]

  def withConfig[T](f: => T) = {
    config.set("name", "lib-event-test")
    f
  }

  "con publish and consume an event" in {
    withConfig {
      println(s"todo")
    }
  }

}
