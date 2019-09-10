package io.flow.event.v2

import com.amazonaws.services.kinesis.AmazonKinesis
import com.amazonaws.services.kinesis.model._
import io.flow.event.Record
import io.flow.lib.event.test.v0.mock.Factories
import io.flow.lib.event.test.v0.models.json._
import io.flow.lib.event.test.v0.models.{TestEvent, TestObject, TestObjectUpserted}
import io.flow.log.RollbarLogger
import io.flow.play.clients.ConfigModule
import org.joda.time.DateTime
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.Inspectors
import org.scalatest.mockito.MockitoSugar
import org.scalatestplus.play.PlaySpec
import org.scalatestplus.play.guice.GuiceOneAppPerSuite
import play.api.Application
import play.api.inject.guice.GuiceApplicationBuilder
import play.api.libs.json.Json

import scala.collection.JavaConverters._
import scala.util.Random

class KinesisProducerSpec extends PlaySpec with MockitoSugar with GuiceOneAppPerSuite with Inspectors with Helpers with KinesisIntegrationSpec {

  private[this] val Utf8: String = "UTF-8"
  private[this] val logger = RollbarLogger.SimpleLogger

  override def fakeApplication(): Application =
    new GuiceApplicationBuilder()
      .bindings(new ConfigModule)
      .build()

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

  "mock KinesisProducer" should {
    "publish one in batch" in {
      val streamConfig = mock[StreamConfig]
      val kinesisClient = mock[AmazonKinesis]

      val mockPutResults = mock[PutRecordsResult]
      when(mockPutResults.getFailedRecordCount).thenReturn(0)
      when(kinesisClient.putRecords(any[PutRecordsRequest]())).thenReturn(mockPutResults)

      val mockDescribeStream = mock[DescribeStreamResult]
      when(mockDescribeStream.getStreamDescription).thenReturn(new StreamDescription().withStreamStatus("ACTIVE"))
      when(kinesisClient.describeStream(any[String]())).thenReturn(mockDescribeStream)

      when(streamConfig.streamName).thenReturn("lib-event-test-stream")
      when(streamConfig.kinesisClient).thenReturn(kinesisClient)

      val producer = new KinesisProducer[TestEvent](streamConfig, numberShards = 1, partitionKeyFieldName = "event_id", logger)

      val event = generateEvent()
      producer.publishBatch(Seq(event))

      val capture: ArgumentCaptor[PutRecordsRequest] = ArgumentCaptor.forClass(classOf[PutRecordsRequest])
      verify(kinesisClient).putRecords(capture.capture())

      capture.getValue.getRecords must have size 1
      val res = capture.getValue.getRecords.get(0)

      val data = new String(res.getData.array(), Utf8)
      data must equal(Json.stringify(Json.toJson(event)))
    }

    "publish multiple in batch" in {
      // 500 events + 5MB limit + 500 events + 100 events

      val streamConfig = mock[StreamConfig]
      val kinesisClient = mock[AmazonKinesis]

      val mockPutResults = mock[PutRecordsResult]
      when(mockPutResults.getFailedRecordCount).thenReturn(0)
      when(kinesisClient.putRecords(any[PutRecordsRequest]())).thenReturn(mockPutResults)

      val mockDescribeStream = mock[DescribeStreamResult]
      when(mockDescribeStream.getStreamDescription).thenReturn(new StreamDescription().withStreamStatus("ACTIVE"))
      when(kinesisClient.describeStream(any[String]())).thenReturn(mockDescribeStream)

      when(streamConfig.streamName).thenReturn("lib-event-test-stream")
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
      capture.getAllValues.get(0).getRecords must have size 500
      capture.getAllValues.get(1).getRecords must have size 5
      capture.getAllValues.get(2).getRecords must have size 500
      capture.getAllValues.get(3).getRecords must have size 100
    }
  }

  "real KinesisProducer" should {
    "put records into kinesis" in {
      withIntegrationQueue { queue =>
        val producer = queue.producer[TestEvent]()

        val sc = queue.streamConfig[TestEvent]
        val client = sc.kinesisClient
        val streamName = sc.streamName

        val event = generateEvent()

        producer.publishBatch(Seq(event))

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

        records.length must be (1)

        val record = Record.fromByteArray(
          arrivalTimestamp = new DateTime(records.head.getApproximateArrivalTimestamp),
          value = records.head.getData.array()
        )

        record.eventId must be(event.eventId)
      }
    }
  }

}
