package io.flow.event.v2

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import io.flow.event.{DynamoStreamEventType, DynamoStreamRecord}
import io.flow.lib.event.test.v0.mock.Factories
import io.flow.lib.event.test.v0.models.json._
import io.flow.lib.event.test.v0.models.{TestObject, TestObjectUpserted}
import io.flow.play.clients.ConfigModule
import org.scalatestplus.play.PlaySpec
import org.scalatestplus.play.guice.GuiceOneAppPerSuite
import play.api.Application
import play.api.inject.guice.GuiceApplicationBuilder

import scala.reflect.runtime.universe._

class DynamoStreamQueueSpec extends PlaySpec with GuiceOneAppPerSuite
  with DynamoStreamHelpers
  with KinesisIntegrationSpec
{

  def dynamoStreamQueue(implicit app: Application) = app.injector.instanceOf[DefaultDynamoStreamQueue]

  override def fakeApplication(): Application =
    new GuiceApplicationBuilder()
      .bindings(new ConfigModule)
      .build()

  "default dynamo queue prevents publishing" in {
    withIntegrationQueue { q =>
      a [RuntimeException] must be thrownBy q.producer[TestObjectUpserted]()
      ()
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
    withIntegrationQueue[TestObject] { q =>
      val testObject = Factories.makeTestObject()
      val eventId = publishTestObject(q, testObject)
      println(s"Published object[$eventId]. Waiting for consumer")

      val fetched = consume[TestObject](q, eventId)
      fetched.isInstanceOf[DynamoStreamRecord] must be (true)
      val record = fetched.asInstanceOf[DynamoStreamRecord]
      record.eventType must be (DynamoStreamEventType.Insert)
      record.recordType must be (typeOf[TestObject])
      record.discriminator must be (Some("io.flow.lib.event.test.v0.models.TestObject"))
      record.newImage must not be empty
      record.newImage.get.get("id") must be (new AttributeValue(testObject.id))

      q.shutdown()
    }
  }

  "table name" in {
    dynamoStreamQueue.tableName[TestObject] must equal("development.test_objects")
  }
}
