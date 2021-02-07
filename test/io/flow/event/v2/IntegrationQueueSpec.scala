package io.flow.event.v2

import io.flow.lib.event.test.v0.models.json._
import io.flow.lib.event.test.v0.models.{TestEvent, TestObject, TestObjectUpserted}
import io.flow.log.RollbarLogger
import io.flow.play.clients.ConfigModule
import io.flow.play.metrics.MockMetricsSystem
import org.scalatestplus.play.PlaySpec
import org.scalatestplus.play.guice.GuiceOneAppPerSuite
import play.api.Application
import play.api.inject.guice.GuiceApplicationBuilder
import software.amazon.awssdk.services.kinesis.model.{GetRecordsRequest, GetShardIteratorRequest, ListShardsRequest, ShardIteratorType}

import java.util.UUID
import java.util.concurrent.atomic.LongAdder
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
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

      val metrics = new MockMetricsSystem()

      val queue = new DefaultQueue(config, creds, endpoints, metrics, rollbar)
      val streamConfig = queue.streamConfig[TestEvent]
      val recordProcessorFactory = KinesisRecordProcessorFactory(streamConfig, _ => (), metrics, rollbar)
      val consumerConfig = new ConsumerConfig(streamConfig, creds.awsSDKv2Creds, recordProcessorFactory)

      consumerConfig.pollingConfig.maxRecords mustBe 1234
      consumerConfig.pollingConfig.idleTimeBetweenReadsInMillis mustBe 4321
      consumerConfig.leaseManagementConfig.maxLeasesForWorker mustBe 8765
      consumerConfig.leaseManagementConfig.maxLeasesToStealAtOneTime mustBe 9012
      consumerConfig.recordsFetcherFactory.idleMillisBetweenCalls mustBe 5678
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

      q.shutdown()
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
      q.shutdownConsumers()
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

      q.shutdown()
    }
  }

  "produce and consume concurrently" in {
    withIntegrationQueue { q =>
      val consumersPoolSize = 1
      val producersPoolSize = 6
      // the json conversion when publishing is quite heavy and therefore makes it hard to use a much bigger number
      val eventsSize = 10000
      val producerContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(producersPoolSize))

      val producer = q.producer[TestEvent]()

      val count = new LongAdder()

      // consume: start [[consumersPoolSize]] consumers consuming concurrently
      (1 to consumersPoolSize).foreach { _ =>
        q.consume[TestEvent](recs => {
          count.add(recs.length.toLong)
        })
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

      q.shutdown()
    }
  }

  "shutdown consumers" in {
    def streamContents(q: DefaultQueue) = {
      val client = q.streamConfig[TestEvent].kinesisClient
      val streamName = q.streamConfig[TestEvent].streamName

      Try {
        val shards = client.listShards(
          ListShardsRequest
            .builder()
            .streamName(streamName)
            .build()
        ).get().shards.asScala

        val iterator = client.getShardIterator(
          GetShardIteratorRequest
            .builder()
            .streamName(streamName)
            .shardId(shards.head.shardId)
            .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
            .build()
        ).get().shardIterator

        val records = client.getRecords(
          GetRecordsRequest
            .builder()
            .shardIterator(iterator)
            .build()
        ).get().records.asScala

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
      deleteStream(q.streamConfig[TestEvent])

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

  "failing in a consumer causes records to be reprocessed" in {
    withIntegrationQueue { queue =>
      deleteStream(queue.streamConfig[TestEvent])

      val producer = queue.producer[TestEvent]()

      val asdf = publishTestObject(producer, TestObject(id = "asdf"))

      var failed = false
      var consumed = false

      queue.consume[TestEvent] { recs =>
        if (failed) {
          recs.head.eventId must be (asdf)
          consumed = true
          ()
        } else {
          failed = true
          throw new Exception("pretend crash")
        }
      }

      eventuallyInNSeconds(120) {
        consumed must be (true)
      }

      queue.shutdownConsumers()
    }
  }

  "failing all retries causes record to be skipped" in {
    withIntegrationQueue { queue =>
      deleteStream(queue.streamConfig[TestEvent])

      val producer = queue.producer[TestEvent]()

      val asdf = publishTestObject(producer, TestObject(id = "asdf"))

      var tries = 0
      var gotHjkl = false

      queue.consume[TestEvent] { recs =>
        if (recs.head.eventId == asdf) {
          tries += 1
          println(s"Try $tries")
          throw new Exception("pretend crash")
        } else {
          gotHjkl = true
        }
      }

      eventuallyInNSeconds(120) {
        tries must be (9)
      }

      publishTestObject(producer, TestObject(id = "hjkl"))

      eventuallyInNSeconds(120) {
        gotHjkl must be (true)
      }

      queue.shutdownConsumers()
    }
  }

  "Events can still be processed even if consumer takes a long time" in {
    withIntegrationQueue { queue =>
      deleteStream(queue.streamConfig[TestEvent])

      val producer = queue.producer[TestEvent]()

      val objs = scala.collection.mutable.ArrayBuffer[String]()

      for (i <- 1 to 10) {
        objs += publishTestObject(producer, TestObject(id = i.toString))
      }

      queue.consume[TestEvent] { recs =>
        Thread.sleep(20 * 1000)
        recs.foreach { rec =>
          objs must contain (rec.eventId)
          objs -= rec.eventId
        }
      }

      eventuallyInNSeconds(120) {
        objs must be (empty)
      }

      queue.shutdownConsumers()
    }
  }

}
