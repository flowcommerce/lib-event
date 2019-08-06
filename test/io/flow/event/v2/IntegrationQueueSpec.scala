package io.flow.event.v2

import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.LongAdder

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SimpleRecordsFetcherFactory
import io.flow.lib.event.test.v0.models.json._
import io.flow.lib.event.test.v0.models.{TestEvent, TestObject, TestObjectUpserted}
import io.flow.log.RollbarLogger
import io.flow.play.clients.ConfigModule
import io.flow.play.metrics.MockMetricsSystem
import org.scalatestplus.play.PlaySpec
import org.scalatestplus.play.guice.GuiceOneAppPerSuite
import play.api.Application
import play.api.inject.guice.GuiceApplicationBuilder

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

class IntegrationQueueSpec extends PlaySpec with GuiceOneAppPerSuite with Helpers with KinesisIntegrationSpec {

  override def fakeApplication(): Application =
    new GuiceApplicationBuilder()
      .bindings(new ConfigModule)
      .build()

  "Passes maxRecords and idleTimeBetweenReadsMs config values to KCL Config" in {
    withConfig { config =>
      config.set("development_workstation.lib.event.test.v0.test_event.json.maxRecords", "1234")
      config.set("development_workstation.lib.event.test.v0.test_event.json.idleMillisBetweenCalls", "5678")
      config.set("development_workstation.lib.event.test.v0.test_event.json.idleTimeBetweenReadsMs", "4321")
      config.set("development_workstation.lib.event.test.v0.test_event.json.maxLeasesForWorker", "8765")
      config.set("development_workstation.lib.event.test.v0.test_event.json.maxLeasesToStealAtOneTime", "9012")
      val creds = new AWSCreds(config)
      val rollbar = RollbarLogger.SimpleLogger
      val endpoints = app.injector.instanceOf[AWSEndpoints]

      val queue = new DefaultQueue(config, creds, endpoints, new MockMetricsSystem(), rollbar)
      val kclConfig = queue.streamConfig[TestEvent].toKclConfig(creds)

      kclConfig.getMaxRecords mustBe 1234
      kclConfig.getIdleTimeBetweenReadsInMillis mustBe 4321
      kclConfig.getMaxLeasesForWorker mustBe 8765
      kclConfig.getMaxLeasesToStealAtOneTime mustBe 9012

      val rff = kclConfig.getRecordsFetcherFactory
      rff mustBe a[SimpleRecordsFetcherFactory]
      val field = classOf[SimpleRecordsFetcherFactory].getDeclaredField("idleMillisBetweenCalls")
      field.setAccessible(true)
      field.get(rff) mustBe 5678
    }
  }

  "can publish and consume an event" in {
    withIntegrationQueue { q =>
      val testObject = TestObject(UUID.randomUUID().toString)

      val producer = q.producer[TestEvent]()

      val eventId = publishTestObject(producer, testObject)
      println(s"Published event[$eventId]. Waiting for consumer")

      val fetched = consume[TestEvent](q, eventId)
      fetched.js.as[TestObjectUpserted].testObject.id must equal(testObject.id)

      q.shutdown
    }
  }

  "keeps track of sequence number" in {
    withIntegrationQueue { q =>
      val testObject1 = TestObject(UUID.randomUUID().toString)

      val producer = q.producer[TestEvent]()

      val eventId1 = publishTestObject(producer, testObject1)
      println(s"Published event[$eventId1]. Waiting for consumer1")

      consume[TestEvent](q, eventId1)

      println(s"SHUTTING DOWN FIRST CONSUMERS")
      Thread.sleep(1000)
      q.shutdownConsumers
      Thread.sleep(20000)

      // Now create a second consumer and verify it does not see testObject1
      val testObject2 = TestObject(UUID.randomUUID().toString)
      val eventId2 = publishTestObject(producer, testObject2)
      val testObject3 = TestObject(UUID.randomUUID().toString)
      val eventId3 = publishTestObject(producer, testObject3)

      println(s"Waiting for eventId3[$eventId3]")
      val all = consumeUntil[TestEvent](q, eventId3)
      println(s"FOUND eventId3[$eventId3]: number records[${all.size}]")
      all.map(_.eventId) must equal(Seq(eventId2, eventId3))

      q.shutdown
    }
  }

  "produce and consume concurrently" in {
    withIntegrationQueue { q =>
      val consumersPoolSize = 1
      val producersPoolSize = 6
      // the json conversion when publishing is quite heavy and therefore makes it hard to huge a much bigger number
      val eventsSize = 10000
      val producerContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(producersPoolSize))
      val consumerContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(consumersPoolSize))

      val producer = q.producer[TestEvent]()

      val count = new LongAdder()

      // consume: start [[consumersPoolSize]] consumers consuming concurrently
      (1 to consumersPoolSize).foreach { _ =>
        Future {
          q.consume[TestEvent](recs => count.add(recs.length.toLong))
        }(consumerContext)
      }

      val start = System.currentTimeMillis()

      // publish concurrently
      (1 to eventsSize/10).foreach { _ =>
        Future {
          // wait 20sec for the consumers to start up
          while (System.currentTimeMillis() - start < 20 * 1000)
            Thread.sleep(100)

          val objs = (1 to 10) map (_ => TestObjectUpserted(
            eventId = eventIdGenerator.randomId(),
            timestamp = org.joda.time.DateTime.now(),
            testObject = TestObject(id = "1")
          ))
          producer.publishBatch(objs)
        }(producerContext)
      }

      // eventually we should have consumed it all
      eventuallyInNSeconds(120) {
        count.longValue() mustBe eventsSize
      }

      q.shutdown
    }
  }

  "shutdown consumers" in {
    def streamContents(q: DefaultQueue) = {
      import com.amazonaws.services.kinesis.model._

      val client = q.streamConfig[TestEvent].kinesisClient
      val streamName = q.streamConfig[TestEvent].streamName

      Try {
        val shards = client.listShards(
          new ListShardsRequest()
            .withStreamName(streamName)
        ).getShards.asScala

        val iterator = client.getShardIterator(
          new GetShardIteratorRequest()
            .withStreamName(streamName)
            .withShardId(shards.head.getShardId)
            .withShardIteratorType(ShardIteratorType.TRIM_HORIZON)
        ).getShardIterator

        val records = client.getRecords(
          new GetRecordsRequest()
            .withShardIterator(iterator)
        ).getRecords.asScala

        records.toList
      } match {
        case Success(recs) =>
          recs
        case Failure(ex) =>
          RollbarLogger.SimpleLogger.warn("Couldn't fetch stream contents", ex)
          Nil
      }
    }

    withIntegrationQueue { q =>
      val sc = q.streamConfig[TestEvent]

      // delete stream
      try {
        sc.kinesisClient.deleteStream(sc.streamName)

        while (sc.kinesisClient.describeStream(sc.streamName).getStreamDescription.getStreamStatus == "DELETING") {
          println("waiting for stream to be deleted")
          Thread.sleep(1000)
        }

      } catch {
        case _: com.amazonaws.services.kinesis.model.ResourceNotFoundException => // ok
      }

      // delete dynamo table
      try {
        val dynamo = AmazonDynamoDBClientBuilder.standard()
          .withCredentials(sc.awsCredentialsProvider)
          .withEndpointConfiguration(
            new EndpointConfiguration(sc.endpoints.dynamodb.get, sc.endpoints.region)
          )
          .build

        dynamo.deleteTable(sc.dynamoTableName)

        while (dynamo.describeTable(sc.dynamoTableName).getTable.getTableStatus == "DELETING") {
          println("waiting for table to be deleted")
          Thread.sleep(1000)
        }
      } catch {
        case _: com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException => // ok
        case _: com.amazonaws.AmazonServiceException => // delete this when https://github.com/localstack/localstack/pull/1461 is merged
      }

      // let's make sure the stream is empty
      streamContents(q) mustBe empty

      val pendingEvents = new LongAdder()

      // produce an element every 1 sec
      val producer = q.producer[TestEvent]()
      val producerRunnable = new Runnable {
        override def run(): Unit = {
          publishTestObject(producer, TestObject(id = "1"))
          pendingEvents.increment()
          ()
        }
      }
      val executor = Executors.newSingleThreadScheduledExecutor()
      executor.scheduleAtFixedRate(producerRunnable, 0, 1, TimeUnit.SECONDS)

      // eventually stream should contain pending elements
      eventuallyInNSeconds(120) {
        pendingEvents.intValue() must be > 1
      }

      // consume
      q.consume[TestEvent](recs => pendingEvents.add(-recs.length.toLong))

      // eventually, stream should be almost empty
      eventuallyInNSeconds(120) {
        pendingEvents.intValue() must be <= 1
      }

      q.shutdownConsumers()

      // eventually stream should not be almost empty any more
      eventuallyInNSeconds(120) {
        pendingEvents.intValue() must be > 1
      }

      // bring in 2 consumers on the same stream
      q.consume[TestEvent](recs => {
        println(s"consumer 1 processing ${recs.length} records")
        pendingEvents.add(-recs.length.toLong)
      })
      q.consume[TestEvent](recs => {
        println(s"consumer 2 processing ${recs.length} records")
        pendingEvents.add(-recs.length.toLong)
      })

      eventuallyInNSeconds(120) {
        pendingEvents.intValue() must be <= 1
      }

      q.shutdownConsumers()

      // eventually stream should not be almost empty any more
      eventuallyInNSeconds(120) {
        pendingEvents.intValue() must be > 1
      }

      executor.shutdown()
    }
  }

}
