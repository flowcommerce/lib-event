package io.flow.event.v2

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import io.flow.event.DynamoStreamRecord
import io.flow.lib.event.test.v0.mock.Factories
import io.flow.lib.event.test.v0.models.json._
import io.flow.lib.event.test.v0.models.{TestObject, TestObjectUpserted}
import io.flow.play.clients.ConfigModule
import io.flow.test.utils.FlowPlaySpec
import play.api.Application
import play.api.inject.guice.GuiceApplicationBuilder

import scala.reflect.runtime.universe._

class DynamoStreamQueueSpec extends FlowPlaySpec
  with DynamoStreamHelpers
  with KinesisIntegrationSpec {

  override def fakeApplication(): Application =
    new GuiceApplicationBuilder()
      .bindings(new ConfigModule)
      .build()

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
      println(s"Published object[$eventId]. Waiting for consumer...")

      val fetched = consume[TestObjectUpserted](q, eventId)
      fetched.js.as[TestObjectUpserted].testObject.id must equal(testObject.testObject.id)
    }
  }

  "can consume an event" in {
    withIntegrationQueue[TestObject] { q =>
      val testObject = Factories.makeTestObject()
      val eventId = publishTestObject(q, testObject)
      println(s"Published object[$eventId]. Waiting for consumer...")

      val fetched = consume[TestObject](q, eventId)
      fetched.isInstanceOf[DynamoStreamRecord] must be (true)
      val record = fetched.asInstanceOf[DynamoStreamRecord]
      record.recordType must be (typeOf[TestObject])
      record.discriminator must be (Some("io.flow.lib.event.test.v0.models.TestObject"))
      record.newImage must not be empty
      record.newImage.get.get("id") must be (new AttributeValue(testObject.id))
    }
  }

  "table name" in {
    dynamoStreamQueue.tableName[TestObject] must equal("development.test_objects")
  }
}
