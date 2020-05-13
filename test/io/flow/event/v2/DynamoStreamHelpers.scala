package io.flow.event.v2

import java.util.concurrent.atomic.AtomicInteger

import com.amazonaws.services.dynamodbv2.model._
import io.flow.event.Record
import io.flow.lib.event.test.v0.models.json._
import io.flow.lib.event.test.v0.models.{TestObject, TestObjectUpserted}
import io.flow.log.RollbarLogger
import io.flow.play.clients.MockConfig
import io.flow.play.metrics.MockMetricsSystem
import play.api.Application

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.reflect.runtime.universe._

trait DynamoStreamHelpers {

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.jdk.CollectionConverters._

  private def config(implicit app: Application) = app.injector.instanceOf[MockConfig]
  private val publishCount = new AtomicInteger()

  private def initTable(stream: DynamoStreamConfig): Unit = {
    val primaryKey = "id"
    val attributeDefinitions: Seq[AttributeDefinition] = Seq(new AttributeDefinition(primaryKey, ScalarAttributeType.S))
    val keySchemaElements: Seq[KeySchemaElement] = Seq(new KeySchemaElement(primaryKey, KeyType.HASH))
    val provisionedThroughput = new ProvisionedThroughput(1000L, 1000L)
    val streamSpecification = new StreamSpecification()
      .withStreamEnabled(true)
      .withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES)
    val request = new CreateTableRequest()
      .withTableName(stream.dynamoTableName)
      .withAttributeDefinitions(attributeDefinitions: _*)
      .withKeySchema(keySchemaElements: _*)
      .withProvisionedThroughput(provisionedThroughput)
      .withStreamSpecification(streamSpecification)
    stream.dynamoDBClient.createTable(request)
    ()
  }

  def withConfig[T](f: MockConfig => T)(implicit app: Application): T = {
    val c = config
    c.set("name", "lib-event-test")
    f(c)
  }

  def withMockQueue[T](f: DynamoStreamQueue => T): T = {
    val rollbar = RollbarLogger.SimpleLogger
    f(new MockDynamoStreamQueue(rollbar))
  }

  def withIntegrationQueue[T: TypeTag](f: DefaultDynamoStreamQueue => Unit)(implicit app: Application): Unit = {
    withConfig { config =>
      val creds = new AWSCreds(config)
      val endpoints = app.injector.instanceOf[AWSEndpoints]
      val metrics = new MockMetricsSystem()
      val rollbar = RollbarLogger.SimpleLogger
      val q = new DefaultDynamoStreamQueue(config, creds, endpoints, metrics, rollbar)
      initTable(q.streamConfig[T])
      f(q)
    }
  }

  def publishTestObject(q: DefaultDynamoStreamQueue, obj: TestObject): String = {
    val stream = q.streamConfig[TestObject]
    val item = Map("id" -> new AttributeValue().withS(obj.id)).asJava
    stream.dynamoDBClient.putItem(stream.dynamoTableName, item)
    publishCount.incrementAndGet().toString
  }

  def publishTestObject(producer: Producer[TestObjectUpserted], obj: TestObjectUpserted): String = {
    producer.publish(obj)
    obj.eventId
  }

  def consume[T: TypeTag](q: Queue, eventId: String, timeoutSeconds: Int = 120): Record = {
    consumeUntil[T](q, eventId, timeoutSeconds).find(_.eventId == eventId).getOrElse {
      sys.error(s"Failed to find eventId[$eventId]")
    }
  }

  def consumeUntil[T: TypeTag](q: Queue, eventId: String, timeoutSeconds: Int = 120): Seq[Record] = {
    val all = scala.collection.mutable.ListBuffer[Record]()
    q.consume[T] { recs =>
      all ++= recs
    }

    Await.result(
      Future {
        while (!all.exists(_.eventId == eventId)) {
          Thread.sleep(100)
        }
      },
      timeoutSeconds.toLong.seconds
    )

    q.shutdown()

    all.toSeq
  }
}
