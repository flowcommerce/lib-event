package io.flow.event.v2

import java.nio.ByteBuffer
import java.util

import com.amazonaws.services.kinesis.model._
import io.flow.event.Util
import play.api.Logger
import play.api.libs.json.{JsValue, Json, Writes}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Random, Success, Try}
import scala.collection.JavaConverters._

case class KinesisProducer[T](
  config: StreamConfig,
  numberShards: Int,
  partitionKeyFieldName: String
) extends Producer[T] {

  import KinesisProducer._

  private[this] val kinesisClient = config.kinesisClient

  setup()

  override def publish[U <: T](event: U)(implicit ec: ExecutionContext, serializer: Writes[U]): Unit =
    publishBatch(Seq(event))

  /**
    * Publishes the events in batch, respecting Kinesis size limitations as defined in
    * <a href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/PutRecords" target="_top">
    *
    * "Each PutRecords request can support up to 500 records. Each record in the request can be as large as 1 MB, up to
    * a limit of 5 MB for the entire request, including partition keys."
    */
  override def publishBatch[U <: T](events: Seq[U])(implicit ec: ExecutionContext, serializer: Writes[U]): Unit = {
    // Make sure that there are events: AWS will complain otherwise
    if (events.nonEmpty) {
      val batchedRecords = new ListBuffer[util.List[PutRecordsRequestEntry]]()
      val firstBatch = new util.ArrayList[PutRecordsRequestEntry](MaxBatchRecordsCount)
      batchedRecords += firstBatch

      events.foldLeft((0L, 0L, firstBatch)) { case ((currentSize, currentBytesSize, currentBatch), evt) =>
        // convert to [[PutRecordsRequestEntry]]
        val event = serializer.writes(evt)
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
    Try(doPublishBatch(entries)) match {
      case Success(response) =>
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
            waitBeforeRetry(attempts)
            publishBatchRetries(toRetries.asJava, attempts + 1)
          }
        }

      case Failure(ex @ (_ : ProvisionedThroughputExceededException | _ : KMSThrottlingException)) if attempts <= MaxRetries =>
        Logger.warn(s"[FlowKinesisWarn] Exception thrown when publishing batch. Retrying $attempts/$MaxRetries ...", ex)
        waitBeforeRetry(attempts)
        publishBatchRetries(entries, attempts + 1)

      case Failure(ex) => throw ex
    }
  }

  private def waitBeforeRetry(attempts: Int): Unit = Thread.sleep((2 + Random.nextInt(2)) * attempts * 1000L)

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

  // 5 MB, counted in decimal as not sure how AWS counts
  // - 100 kB as an arbitrary margin to avoid errors (2% of 5MB - not a big difference)
  val MaxBatchRecordsSizeBytes: Long = 5L * 1000 * 1000 - 100L * 1000

  // Let's really retry!
  val MaxRetries = 10
}
