package io.flow.event.v2

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import io.flow.event.{DynamoStreamEventName, DynamoStreamRecord}
import io.flow.lib.event.test.v0.mock.Factories
import io.flow.lib.event.test.v0.models.json._
import io.flow.lib.event.test.v0.models.{TestObject, TestObjectUpserted}
import io.flow.play.clients.ConfigModule
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.play.PlaySpec
import org.scalatestplus.play.guice.GuiceOneAppPerSuite
import play.api.Application
import play.api.inject.guice.GuiceApplicationBuilder

import scala.reflect.runtime.universe._

class DynamoStreamQueueSpec extends PlaySpec with GuiceOneAppPerSuite
  with DynamoStreamHelpers
  with KinesisIntegrationSpec
  with BeforeAndAfterAll {

  override def fakeApplication(): Application =
    new GuiceApplicationBuilder()
      .bindings(new ConfigModule)
      .build()

  override def beforeAll(): Unit = {
    super.beforeAll()
    initTable()
  }

  "default dynamo queue prevents publishing" in {
    withIntegrationQueue { q =>
      a [RuntimeException] must be thrownBy q.producer[TestObjectUpserted]()
    }
  }

  "can publish and consume an event using mock" in {
    withMockQueue { q =>
      val testObject = Factories.makeTestObjectUpserted()
      val producer = q.producer[TestObjectUpserted]()
      val eventId = publishTestObject(producer, testObject)
      println(s"Published object[$eventId]. Waiting for consumer")

      val fetched = consume[TestObjectUpserted](q, eventId)
      fetched.js.as[TestObjectUpserted].testObject.id must equal(testObject.testObject.id)

      q.shutdown()
    }
  }

  "can consume an event" in {
    withIntegrationQueue { q =>
      val testObject = Factories.makeTestObject()
      val eventId = publishTestObject(testObject)
      println(s"Published object[$eventId]. Waiting for consumer")

      val fetched = consume[TestObject](q, eventId)
      fetched.isInstanceOf[DynamoStreamRecord] must be (true)
      val record = fetched.asInstanceOf[DynamoStreamRecord]
      record.eventName must be (DynamoStreamEventName.Insert)
      record.recordType must be (typeOf[TestObject])
      record.discriminator must be (Some("TestObject"))
      record.newImage must not be (empty)
      record.newImage("id") must be (new AttributeValue(testObject.id))

      q.shutdown()
    }
  }
}
