package io.flow.event

import io.flow.event.Util
import org.scalatest.{FunSpec, Matchers}
import play.api.libs.json.Json

class UtilSpec extends FunSpec with Matchers {

  it("must parse long") {
    Util.mustParseLong(Json.parse("""{"id": 123}"""), "id") should be (123)
  }

  it("must parse max long") {
    Util.mustParseLong(Json.parse(s"""{"id": ${Long.MaxValue}}"""), "id") should be (Long.MaxValue)
  }

  it("fail to parse value that is not a long") {
    Util.parseLong(Json.parse("""{"id": "123"}"""), "id") should be (None)
  }

  it("fail to parse value for field that does not exist") {
    Util.parseLong(Json.parse("""{"bogus": 123}"""), "id") should be (None)
  }

}
