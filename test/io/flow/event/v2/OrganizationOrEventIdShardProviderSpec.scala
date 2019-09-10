package io.flow.event.v2

import org.scalatest.{MustMatchers, WordSpec}
import play.api.libs.json.{JsObject, JsString, Json}

class OrganizationOrEventIdShardProviderSpec extends WordSpec with MustMatchers {

  case class CaseClass(i: Int, s: String)

  "OrganizationOrEventIdShardProvider" should {

    "cast" in {
      val js = JsObject(Map("event_id" -> JsString("abc")))

      OrganizationOrEventIdShardProvider[Int].get(1, js) mustBe "abc"
      OrganizationOrEventIdShardProvider[String].get("", js) mustBe "abc"
      OrganizationOrEventIdShardProvider[Any].get(123, js) mustBe "abc"
      OrganizationOrEventIdShardProvider[CaseClass].get(CaseClass(1, "a"), js) mustBe "abc"
      OrganizationOrEventIdShardProvider[(Long, Long)].get((1L, 2L), js) mustBe "abc"
    }

    "provide shard" in {

      val org = Json.parse(""" { "event_id" : "e", "organization": "o", "data": "d" } """)
      OrganizationOrEventIdShardProvider[Any].get("", org) mustBe "o"

      val orgId = Json.parse(""" { "event_id" : "e", "organization_id": "oi", "data": "d" } """)
      OrganizationOrEventIdShardProvider[Any].get("", orgId) mustBe "oi"

      val eventId = Json.parse(""" { "event_id" : "e", "no_org": "oi", "data": "d" } """)
      OrganizationOrEventIdShardProvider[Any].get("", eventId) mustBe "e"

      val error = Json.parse(""" { "no_event" : "e", "no_org": "oi", "data": "d" } """)

      an[Exception] mustBe thrownBy(OrganizationOrEventIdShardProvider[Any].get("", error))
    }

  }

}
