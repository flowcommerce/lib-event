package io.flow.event.v2

import java.util.UUID

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SimpleRecordsFetcherFactory
import io.flow.lib.event.test.v0.models.{TestEvent, TestObject, TestObjectUpserted}
import io.flow.lib.event.test.v0.models.json._
import io.flow.log.RollbarLogger
import io.flow.play.clients.ConfigModule
import io.flow.play.metrics.MockMetricsSystem
import org.scalatestplus.play.guice.GuiceOneAppPerSuite
import org.scalatestplus.play.PlaySpec
import play.api.Application
import play.api.inject.guice.GuiceApplicationBuilder

class QueueSpec extends PlaySpec with GuiceOneAppPerSuite with Helpers with KinesisIntegrationSpec {

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
    withConfig { config =>
      val testObject = TestObject(UUID.randomUUID().toString)

      val creds = new AWSCreds(config)
      val rollbar = RollbarLogger.SimpleLogger
      val endpoints = app.injector.instanceOf[AWSEndpoints]

      val q = new DefaultQueue(config, creds, endpoints, new MockMetricsSystem(), rollbar)
      val producer = q.producer[TestEvent]()

      val eventId = publishTestObject(producer, testObject)
      println(s"Published event[$eventId]. Waiting for consumer")

      val fetched = consume[TestEvent](q, eventId)
      fetched.js.as[TestObjectUpserted].testObject.id must equal(testObject.id)

      q.shutdown
    }
  }

  "keeps track of sequence number" in {
    withConfig { config =>
      val testObject1 = TestObject(UUID.randomUUID().toString)

      val creds = new AWSCreds(config)
      val rollbar = RollbarLogger.SimpleLogger
      val endpoints = app.injector.instanceOf[AWSEndpoints]

      val q = new DefaultQueue(config, creds, endpoints, new MockMetricsSystem(), rollbar)
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

}
