package io.flow.event

import org.scalatest.{FunSpec, Matchers}
import play.api.libs.json.{JsString, Json}

class UtilSpec extends FunSpec with Matchers {

  it("should extract a string from a JsObject") {
    val obj = Json.obj(
      ("name" -> JsString("val"))
    )
    Util.parseString(obj, "name") shouldBe Some("val")
  }

  it("should handle a JsValue that isn't an object") {
    Util.parseString(JsString("name"), "name") shouldBe None
  }

}
