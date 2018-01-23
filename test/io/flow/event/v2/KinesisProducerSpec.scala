package io.flow.event.v2

import java.nio.charset.Charset

import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model.{PutRecordsRequest, PutRecordsResult}
import io.flow.lib.event.test.v0.mock.Factories
import io.flow.lib.event.test.v0.models.json._
import io.flow.lib.event.test.v0.models.{TestEvent, TestObject, TestObjectUpserted}
import org.apache.commons.io.Charsets
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Inspectors, Matchers}
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

class KinesisProducerSpec extends FlatSpec with Matchers with MockitoSugar with Inspectors {

  val utf8: Charset = Charsets.UTF_8

  def generateEvent(bytes: Int = 200): TestObjectUpserted = {
    val base = Factories.makeTestObjectUpserted().copy(testObject = TestObject(""))
    val baseBytes = Json.toJson(base).toString().getBytes(utf8).length
    val remainingBytes = bytes - baseBytes
    if (remainingBytes < 0) throw new IllegalArgumentException(s"$bytes must be >= $baseBytes")
    val id: String = Random.alphanumeric.take(remainingBytes).mkString
    base.copy(testObject = TestObject(id))
  }

  "KinesisProducerSpec" should "provide a helper function to create event with json payload of a specific size" in {
    val sizes: Seq[Int] = Seq(200, 500, 1024, 1024 * 1024, 10 * 1024 * 1024)
    forAll(sizes) { bytes =>
      Json.stringify(Json.toJson(generateEvent(bytes))).getBytes(utf8) should have size bytes
    }
  }

  "KinesisProducer" should "publish one in batch" in {
    val streamConfig = mock[StreamConfig]
    val kinesisClient = mock[AmazonKinesis]
    val mockPutResults = mock[PutRecordsResult]
    when(mockPutResults.getFailedRecordCount).thenReturn(0)
    when(kinesisClient.putRecords(any())).thenReturn(mockPutResults)
    when(streamConfig.kinesisClient).thenReturn(kinesisClient)

    val producer = new KinesisProducer[TestEvent](streamConfig, numberShards = 1, partitionKeyFieldName = "event_id")

    val event = generateEvent()
    producer.publishBatch(Seq(event))

    val capture: ArgumentCaptor[PutRecordsRequest] = ArgumentCaptor.forClass(classOf[PutRecordsRequest])
    verify(kinesisClient).putRecords(capture.capture())

    capture.getValue.getRecords should have size 1
    val res = capture.getValue.getRecords.get(0)

    val data = new String(res.getData.array(), utf8)
    data shouldBe Json.stringify(Json.toJson(event))
  }

  it should "publish multiple in batch" in {
    // 500 events + 5MB limit + 500 events + 100 events

    val streamConfig = mock[StreamConfig]
    val kinesisClient = mock[AmazonKinesis]
    val mockPutResults = mock[PutRecordsResult]
    when(mockPutResults.getFailedRecordCount).thenReturn(0)
    when(kinesisClient.putRecords(any())).thenReturn(mockPutResults)
    when(streamConfig.kinesisClient).thenReturn(kinesisClient)

    val producer = new KinesisProducer[TestEvent](streamConfig, numberShards = 1, partitionKeyFieldName = "event_id")

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

    capture.getAllValues should have size 4
    capture.getAllValues.get(0).getRecords should have size 500
    capture.getAllValues.get(1).getRecords should have size 5
    capture.getAllValues.get(2).getRecords should have size 500
    capture.getAllValues.get(3).getRecords should have size 100
  }

}
