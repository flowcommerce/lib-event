package io.flow.event.v2

import java.nio.ByteBuffer
import java.util

import com.amazonaws.services.kinesis.model._
import io.flow.event.Util
import play.api.Logger
import play.api.libs.json.{JsValue, Json}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}


case class KinesisProducer(
  config: StreamConfig,
  numberShards: Int,
  partitionKeyFieldName: String
) extends Producer {

  import KinesisProducer._

  private[this] val kinesisClient = config.kinesisClient

  setup()

  override def publish(event: JsValue)(implicit ec: ExecutionContext) {
    val partitionKey = Util.mustParseString(event, partitionKeyFieldName)
    val bytes = Json.stringify(event).getBytes("UTF-8")

    kinesisClient.putRecord(
      new PutRecordRequest()
        .withData(ByteBuffer.wrap(bytes))
        .withPartitionKey(partitionKey)
        .withStreamName(config.streamName)
    )
  }

  /**
    * Publishes the events in batch, respecting Kinesis size limitations as defined in
    * <a href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/PutRecords" target="_top">
    *
    * "Each PutRecords request can support up to 500 records. Each record in the request can be as large as 1 MB, up to
    * a limit of 5 MB for the entire request, including partition keys."
    */
  override def publishBatch(events: Seq[JsValue])(implicit ec: ExecutionContext): Unit = {

    val batchedRecords = new ListBuffer[util.List[PutRecordsRequestEntry]]()
    val firstBatch = new util.ArrayList[PutRecordsRequestEntry](MaxBatchRecordsCount)
    batchedRecords += firstBatch

    events.foldLeft((0L, 0L, firstBatch)) { case ((currentSize, currentBytesSize, currentBatch), event) =>
      // convert to [[PutRecordsRequestEntry]]
      val partitionKey = Util.mustParseString(event, partitionKeyFieldName)
      val data = Json.stringify(event).getBytes("UTF-8")
      val record = new PutRecordsRequestEntry().withPartitionKey(partitionKey).withData(ByteBuffer.wrap(data))

      // did the current batch reach one of the limitations?
      val newBytesSize = currentBytesSize + data.length
      if (currentSize == MaxBatchRecordsCount || newBytesSize > MaxBatchRecordsSizeBytes) {
        val newBatch = new util.ArrayList[PutRecordsRequestEntry](MaxBatchRecordsCount)
        batchedRecords += newBatch
        newBatch.add(record)
        (1L, data.length, newBatch)
      } else {
        currentBatch.add(record)
        (currentSize + 1, newBytesSize, currentBatch)
      }
    }

    batchedRecords.foreach { batch =>
      val putRecordsRequest = new PutRecordsRequest().withStreamName(config.streamName).withRecords(batch)
      kinesisClient.putRecords(putRecordsRequest)
    }

  }

  override def shutdown(implicit ec: ExecutionContext): Unit = {
    kinesisClient.shutdown()
  }


  /**
    * Sets up the stream name in ec2, either an error or Unit
    **/
  private[this] def setup() {
    Try {
      kinesisClient.createStream(
        new CreateStreamRequest()
          .withStreamName(config.streamName)
          .withShardCount(numberShards)
      )
    } match {
      case Success(_) => {
        // All good
      }

      case Failure(ex) => {
        ex match {
          case _: ResourceInUseException => {
            // do nothing... already exists, ignore
          }

          case e: Throwable => {
            val msg = s"FlowKinesisError [${this.getClass.getName}] Stream[$config.streamName] could not be created. Error Message: ${e.getMessage}"
            Logger.warn(msg)
            e.printStackTrace(System.err)
            sys.error(msg)
          }
        }
      }
    }
  }
}

object KinesisProducer {
  val MaxBatchRecordsCount = 500
  val MaxBatchRecordsSizeBytes = 5 * 1024 * 1024
}
