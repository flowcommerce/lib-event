package io.flow.event.v2

import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model.PutRecordsRequest
import org.apache.commons.io.Charsets
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Inspectors, Matchers}
import play.api.libs.json.{JsValue, Json}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

class KinesisProducerSpec extends FlatSpec with Matchers with MockitoSugar with Inspectors {

  val utf8 = Charsets.UTF_8

  def generateJson(partitionKey: String = "p", bytes: Int = 20): JsValue = {
    val fixedSize = 7  // size of: {"":""}
    val size = bytes - fixedSize - partitionKey.getBytes(utf8).length
    val key: String = Random.alphanumeric.take(size).mkString
    val js: String = s"""{"$partitionKey":"$key"}"""
    Json.parse(js)
  }

  "KinesisProducerSpec" should "provide a helper function to create json payload of a specific size" in {
    val sizes: Seq[Int] = Seq(20, 100, 1024, 1024 * 1024, 10 * 1024 * 1024)
    forAll(sizes) { bytes =>
      Json.stringify(generateJson(bytes = bytes)).getBytes(utf8) should have size bytes
    }
  }

  "KinesisProducer" should "publish one in batch" in {
    val streamConfig = mock[StreamConfig]
    val kinesisClient = mock[AmazonKinesis]
    when(streamConfig.kinesisClient).thenReturn(kinesisClient)

    val producer = new KinesisProducer(streamConfig, numberShards = 1, partitionKeyFieldName = "p")

    val json = generateJson()
    producer.publishBatch(Seq(json))

    val capture: ArgumentCaptor[PutRecordsRequest] = ArgumentCaptor.forClass(classOf[PutRecordsRequest])
    verify(kinesisClient).putRecords(capture.capture())

    capture.getValue.getRecords should have size 1
    val res = capture.getValue.getRecords.get(0)

    val data = new String(res.getData.array(), utf8)
    data shouldBe Json.stringify(json)
  }

  it should "publish multiple in batch" in {
    // 500 events + 5MB limit + 500 events + 100 events

    val streamConfig = mock[StreamConfig]
    val kinesisClient = mock[AmazonKinesis]
    when(streamConfig.kinesisClient).thenReturn(kinesisClient)

    val producer = new KinesisProducer(streamConfig, numberShards = 1, partitionKeyFieldName = "p")

    val events = {
      (1 to 500).map(_ => generateJson()) ++
        // limit is 5 * 1024 * 1024 so let's generate 5 events of size 1024 * 1024
        (1 to 5).map(_ => generateJson(bytes = 1024 * 1024)) ++
        (1 to 500).map(_ => generateJson()) ++
        (1 to 100).map(_ => generateJson())
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
