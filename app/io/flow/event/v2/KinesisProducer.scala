package io.flow.event.v2

import java.nio.ByteBuffer
import java.util

import com.amazonaws.services.kinesis.model._
import io.flow.event.Util
import play.api.Logger
import play.api.libs.json.{JsValue, Json}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Random, Success, Try}
import scala.collection.JavaConverters._

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

    val record =
      new PutRecordRequest()
      .withData(ByteBuffer.wrap(bytes))
      .withPartitionKey(partitionKey)
      .withStreamName(config.streamName)

    publishRetries(record, 1)
  }

  private def publishRetries(record: PutRecordRequest, attempts: Int): Unit = {
    Try(doPublish(record))
      .recover {
        case _: ProvisionedThroughputExceededException if attempts <= MaxRetries =>
          Logger.warn(s"[FlowKinesisWarn] record failed to be published. " +
            s"Retrying $attempts/$MaxRetries ...")
          Thread.sleep((2 + Random.nextInt(2)) * attempts * 1000L)
          publishRetries(record, attempts + 1)
        case e => Logger.error("[FlowKinesisError] record failed to be published", e)
      }.get // open the pandora box
  }

  private def doPublish(record: PutRecordRequest): PutRecordResult = kinesisClient.putRecord(record)

  /**
    * Publishes the events in batch, respecting Kinesis size limitations as defined in
    * <a href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/PutRecords" target="_top">
    *
    * "Each PutRecords request can support up to 500 records. Each record in the request can be as large as 1 MB, up to
    * a limit of 5 MB for the entire request, including partition keys."
    */
  override def publishBatch(events: Seq[JsValue])(implicit ec: ExecutionContext): Unit = {
    // Make sure that there are events: AWS will complain otherwise
    if (events.nonEmpty) {
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

      batchedRecords.foreach(publishBatchRetries(_, 1))
    }
  }

  @tailrec
  private def publishBatchRetries(entries: util.List[PutRecordsRequestEntry], attempts: Int): Unit = {
    val response = doPublishBatch(entries)

    val failedRecordCount = response.getFailedRecordCount
    if (failedRecordCount > 0) {
      if (attempts > MaxRetries) {
        // log errors
        val errorMessage = s"[FlowKinesisError] $failedRecordCount/${entries.size()} failed to be published"
        Logger.error(errorMessage)
        response.getRecords.asScala.foreach { resultEntry =>
          if (Option(resultEntry.getErrorCode).isDefined || Option(resultEntry.getErrorMessage).isDefined)
            Logger.error(s"[FlowKinesisError] $resultEntry")
        }

        sys.error(errorMessage)

      } else {
        Logger.warn(s"[FlowKinesisWarn] $failedRecordCount/${entries.size()} failed to be published. " +
          s"Retrying $attempts/$MaxRetries ...")

        val toRetries =
          entries.asScala.zip(response.getRecords.asScala)
            .collect { case (entry, res) if Option(res.getErrorCode).isDefined || Option(res.getErrorMessage).isDefined => entry }
        Thread.sleep((2 + Random.nextInt(2)) * attempts * 1000L)
        publishBatchRetries(toRetries.asJava, attempts + 1)
      }
    }
  }

  private def doPublishBatch(entries: util.List[PutRecordsRequestEntry]): PutRecordsResult = {
    val putRecordsRequest = new PutRecordsRequest().withStreamName(config.streamName).withRecords(entries)
    kinesisClient.putRecords(putRecordsRequest)
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

  // Let's really retry!
  val MaxRetries = 10
}
