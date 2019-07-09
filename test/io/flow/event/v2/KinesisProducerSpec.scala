package io.flow.event.v2

import java.util.concurrent.CompletableFuture

import io.flow.lib.event.test.v0.mock.Factories
import io.flow.lib.event.test.v0.models.json._
import io.flow.lib.event.test.v0.models.{TestEvent, TestObject, TestObjectUpserted}
import io.flow.log.RollbarLogger
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.Inspectors
import org.scalatest.mockito.MockitoSugar
import org.scalatestplus.play.PlaySpec
import play.api.libs.json.Json
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.{PutRecordsRequest, PutRecordsResponse}

import scala.util.Random

class KinesisProducerSpec extends PlaySpec with MockitoSugar with Inspectors {

  private[this] val Utf8: String = "UTF-8"
  private[this] val logger = new RollbarLogger(rollbar = None, attributes = Map.empty, legacyMessage = None)

  def generateEvent(bytes: Int = 200): TestObjectUpserted = {
    val base = Factories.makeTestObjectUpserted().copy(testObject = TestObject(""))
    val baseBytes = Json.toJson(base).toString().getBytes(Utf8).length
    val remainingBytes = bytes - baseBytes
    if (remainingBytes < 0) throw new IllegalArgumentException(s"$bytes must be >= $baseBytes")
    val id: String = Random.alphanumeric.take(remainingBytes).mkString
    base.copy(testObject = TestObject(id))
  }

  "KinesisProducerSpec should provide a helper function to create event with json payload of a specific size" in {
    val sizes: Seq[Int] = Seq(200, 500, 1024, 1024 * 1024, 10 * 1024 * 1024)
    forAll(sizes) { bytes =>
      Json.stringify(Json.toJson(generateEvent(bytes))).getBytes(Utf8) must have size bytes.toLong
    }
  }

  "KinesisProducer should publish one in batch" in {
    val streamConfig = mock[StreamConfig]
    val kinesisClient = mock[KinesisAsyncClient]
    val mockPutResults = PutRecordsResponse.builder().failedRecordCount(0).build()
    when(kinesisClient.putRecords(any[PutRecordsRequest]())).thenReturn(CompletableFuture.completedFuture(mockPutResults))
    when(streamConfig.kinesisClient).thenReturn(kinesisClient)

    val producer = new KinesisProducer[TestEvent](streamConfig, numberShards = 1, partitionKeyFieldName = "event_id", logger)

    val event = generateEvent()
    producer.publishBatch(Seq(event))

    val capture: ArgumentCaptor[PutRecordsRequest] = ArgumentCaptor.forClass(classOf[PutRecordsRequest])
    verify(kinesisClient).putRecords(capture.capture())

    capture.getValue.records must have size 1
    val res = capture.getValue.records.get(0)

    val data = new String(res.data.asByteArray(), Utf8)
    data must equal(Json.stringify(Json.toJson(event)))
  }

  "publish multiple in batch" in {
    // 500 events + 5MB limit + 500 events + 100 events

    val streamConfig = mock[StreamConfig]
    val kinesisClient = mock[KinesisAsyncClient]
    val mockPutResults = PutRecordsResponse.builder().failedRecordCount(0).build()
    when(kinesisClient.putRecords(any[PutRecordsRequest]())).thenReturn(CompletableFuture.completedFuture(mockPutResults))
    when(streamConfig.kinesisClient).thenReturn(kinesisClient)

    val producer = new KinesisProducer[TestEvent](streamConfig, numberShards = 1, partitionKeyFieldName = "event_id", logger)

    val events = {
      (1 to 500).map(_ => generateEvent()) ++
        // limit is 5MB - 100kB so let's generate 5 events of size (5MB - 100kB) / 5
        (1 to 5).map(_ => generateEvent(bytes = KinesisProducer.MaxBatchRecordsSizeBytes.toInt / 5)) ++
        (1 to 500).map(_ => generateEvent()) ++
        (1 to 100).map(_ => generateEvent())
    }
    producer.publishBatch(events)

    val capture: ArgumentCaptor[PutRecordsRequest] = ArgumentCaptor.forClass(classOf[PutRecordsRequest])
    verify(kinesisClient, times(4)).putRecords(capture.capture())

    capture.getAllValues must have size 4
    capture.getAllValues.get(0).records must have size 500
    capture.getAllValues.get(1).records must have size 5
    capture.getAllValues.get(2).records must have size 500
    capture.getAllValues.get(3).records must have size 100
  }

}
