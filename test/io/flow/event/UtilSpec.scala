package io.flow.event


import org.scalatest.{FunSpec, Matchers}
import play.api.libs.json.Json

import scala.util.{Failure, Success, Try}

class UtilSpec extends FunSpec with Matchers {

  it("must parse long") {
    Util.mustParseLong(Json.obj("id" -> 123), "id") should be (123)
  }

  it("must parse number string as long") {
    val numberAsString = "123"
    Util.mustParseLong(Json.obj("id" -> numberAsString), "id") should be (123)
  }

  it("must parse max long") {
    Util.mustParseLong(Json.obj("id" -> Long.MaxValue), "id") should be (Long.MaxValue)
  }

  it("fail to parse value larger than long") {
    val largeNumber = "12392398349349083459083459083409582093485902345"
    Try {
      Util.mustParseLong(Json.obj("id" -> largeNumber), "id")
    } match {
      case Success(_) => fail("Test should have failed")
      case Failure(_) => assert(true)
    }
  }

  it("fail to parse character string as long") {
    val charStr = "abc"
    Try {
      Util.mustParseLong(Json.obj("id" -> charStr), "id")
    } match {
      case Success(_) => fail("Test should have failed")
      case Failure(_) => assert(true)
    }
  }

  it("fail to parse value for field that does not exist") {
    Try {
      Util.mustParseLong(Json.obj("bogus" -> 123), "id")
    } match {
      case Success(_) => fail("Test should have failed")
      case Failure(_) => assert(true)
    }
  }

}
