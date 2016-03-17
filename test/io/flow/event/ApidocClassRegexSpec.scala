package io.flow.event

import lib.RegexHelpers
import org.scalatestplus.play.{PlaySpec, OneAppPerSuite}


class ApidocClassRegexSpec extends PlaySpec with OneAppPerSuite {
  "apidocclass regex" should {
    "validate a service with single name" in {
      val path = "io.flow.sample.v0.models.Event"

      val isMatch = RegexHelpers.ApidocClass.findFirstIn(path) match {
        case Some(s) => true
        case None => false
      }

      isMatch must equal(true)
    }

    "validate a service with multi name" in {
      val path = "io.flow.sample.multi.v0.models.Event"

      val isMatch = RegexHelpers.ApidocClass.findFirstIn(path) match {
        case Some(s) => true
        case None => false
      }

      isMatch must equal(true)
    }

    "validate a service with long multi name" in {
      val path = "io.flow.sample.multia.multib.v0.models.Event"

      val isMatch = RegexHelpers.ApidocClass.findFirstIn(path) match {
        case Some(s) => true
        case None => false
      }

      isMatch must equal(true)
    }

    "invalidate a service with invalid match" in {
      val path = "io.flow.sample.v0.Event"

      val isMatch = RegexHelpers.ApidocClass.findFirstIn(path) match {
        case Some(s) => true
        case None => false
      }

      isMatch must equal(false)
    }
  }
}
