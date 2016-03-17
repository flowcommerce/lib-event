package io.flow.event

import io.flow.play.util.FlowEnvironment
import org.scalatestplus.play.{PlaySpec, OneAppPerSuite}

class StreamNamesSpec extends PlaySpec with OneAppPerSuite {

  private[this] val dev = StreamNames(FlowEnvironment.Development)

  "validate a service with single name" in {
    dev.json("io.flow.sample.v0.models.Event") must equal(Some("development.sample.v0.event.json"))
  }

  "production environment is in the stream name" in {
    StreamNames(FlowEnvironment.Production).json("io.flow.sample.v0.models.Event") must equal(Some("production.sample.v0.event.json"))
  }

  "validate a service with multi name" in {
    dev.json("io.flow.sample.multi.v0.models.Event") must equal(Some("development.sample.multi.v0.event.json"))
  }

  "validate a service with long multi name" in {
    dev.json("io.flow.sample.multia.multib.v0.models.Event") must equal(Some("development.sample.multia.multib.v0.event.json"))
  }

  "invalidate a service with invalid match" in {
    dev.json("io.flow.sample.v0.Event") must be(None)
  }

}
