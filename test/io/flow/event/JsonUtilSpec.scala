package io.flow.event

import play.api.libs.json.Json

import scala.util.{Failure, Success, Try}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class JsonUtilSpec extends AnyFunSpec with Matchers {

  private[this] def validateError(expected: String)(f: => Any) = {
    Try {
      f
    } match {
      case Success(_) => fail("Test should have failed")
      case Failure(ex) => ex.getMessage should be(expected)
    }
  }

  it("must parse long") {
    JsonUtil.requiredLong(Json.obj("id" -> 123), "id") should be (123)
  }

  it("must parse number string as long") {
    JsonUtil.requiredLong(Json.obj("id" -> "123"), "id") should be (123)
  }

  it("must parse max long") {
    JsonUtil.requiredLong(Json.obj("id" -> Long.MaxValue), "id") should be (Long.MaxValue)
  }

  it("fail to parse value larger than long") {
    validateError("Field[id] has invalid value[12392398349349083459083459083409582093485902345]: must be a long") {
      JsonUtil.requiredLong(Json.obj("id" -> "12392398349349083459083459083409582093485902345"), "id")
    }
  }

  it("fail to parse character string as long") {
    validateError("Field[id] has invalid value[abc]: must be a long") {
      JsonUtil.requiredLong(Json.obj("id" -> "abc"), "id")
    }
  }

  it("fail to parse value for field that does not exist") {
    validateError("Field[id] is required") {
      JsonUtil.requiredLong(Json.obj("bogus" -> 123), "id")
    }
  }

  it("optionalString ignores whitespace") {
    JsonUtil.optionalString(Json.obj("id" -> "  "), "id") should be(None)
    JsonUtil.optionalString(Json.obj("id" -> " foo "), "id") should be(Some("foo"))
  }

  it("requiredString ignores whitespace") {
    validateError("Field[id] is required") {
      JsonUtil.requiredString(Json.obj(), "id")
    }

    validateError("Field[id] cannot be empty") {
      JsonUtil.requiredString(Json.obj("id" -> "  "), "id")
    }

    JsonUtil.requiredString(Json.obj("id" -> " foo "), "id") should be("foo")
  }

  it("requiredPositiveLong") {
    JsonUtil.requiredPositiveLong(Json.obj("id" -> "5"), "id") should be(5L)

    validateError("Field[id] has invalid value[-5]: must be > 0") {
      JsonUtil.requiredPositiveLong(Json.obj("id" -> "-5"), "id")
    }
  }

  it("optionalPositiveLong") {
    JsonUtil.optionalPositiveLong(Json.obj(), "id") should be(None)
    JsonUtil.optionalPositiveLong(Json.obj("id" -> ""), "id") should be(None)
    JsonUtil.optionalPositiveLong(Json.obj("id" -> "5"), "id") should be(Some(5L))

    validateError("Field[id] has invalid value[-5]: must be > 0") {
      JsonUtil.optionalPositiveLong(Json.obj("id" -> "-5"), "id")
    }
  }

  it("requiredPositiveInt") {
    JsonUtil.requiredPositiveInt(Json.obj("id" -> "5"), "id") should be(5)

    validateError("Field[id] has invalid value[-5]: must be > 0") {
      JsonUtil.requiredPositiveInt(Json.obj("id" -> "-5"), "id")
    }
  }

  it("requiredBoolean") {
    JsonUtil.requiredBoolean(Json.obj("id" -> "true"), "id") should be(true)
    JsonUtil.requiredBoolean(Json.obj("id" -> true), "id") should be(true)
    JsonUtil.requiredBoolean(Json.obj("id" -> "t"), "id") should be(true)

    JsonUtil.requiredBoolean(Json.obj("id" -> "false"), "id") should be(false)
    JsonUtil.requiredBoolean(Json.obj("id" -> false), "id") should be(false)
    JsonUtil.requiredBoolean(Json.obj("id" -> "f"), "id") should be(false)

    validateError("Field[id] has invalid value[5]. Use true, t, false, or f") {
      JsonUtil.requiredBoolean(Json.obj("id" -> "5"), "id")
    }
  }
}
