package io.flow.event.v2

import java.nio.ByteBuffer
import java.util

import com.amazonaws.services.kinesis.model._
import com.github.ghik.silencer.silent
import io.flow.log.RollbarLogger
import org.apache.http.NoHttpResponseException
import play.api.libs.json.{Json, Writes}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal
import scala.util.{Failure, Random, Success, Try}

case class KinesisProducer[T](
  config: StreamConfig,
  numberShards: Int,
  logger: RollbarLogger
) extends Producer[T] with StreamUsage {

  import KinesisProducer._

  private[this] val kinesisClient = config.kinesisClient

  private[this] val logger_ =
    logger
      .withKeyValue("class", this.getClass.getName)
      .withKeyValue("stream", config.streamName)

  setup()

  override def publish[U <: T](
    event: U,
    shardFinder: KinesisShardProvider[U] = OrganizationOrEventIdShardProvider[U]
  )(implicit serializer: Writes[U]): Unit =
    publishBatch(Seq(event), shardFinder)

  /**
    * Publishes the events in batch, respecting Kinesis size limitations as defined in
    * <a href="http://docs.aws.amazon.com/goto/WebAPI/kinesis-2013-12-02/PutRecords" target="_top">
    *
    * "Each PutRecords request can support up to 500 records. Each record in the request can be as large as 1 MB, up to
    * a limit of 5 MB for the entire request, including partition keys."
    */
  override def publishBatch[U <: T](
    events: Seq[U],
    shardProvider: KinesisShardProvider[U] = OrganizationOrEventIdShardProvider[U]
  )(implicit serializer: Writes[U]): Unit = {
    // Make sure that there are events: AWS will complain otherwise
    if (events.nonEmpty) {
      val batchedRecords = new ListBuffer[util.List[PutRecordsRequestEntry]]()
      val firstBatch = new util.ArrayList[PutRecordsRequestEntry](MaxBatchRecordsCount)
      batchedRecords += firstBatch

      events.foldLeft((0L, 0L, firstBatch)) { case ((currentSize, currentBytesSize, currentBatch), evt) =>
        // convert to [[PutRecordsRequestEntry]]
        val event = serializer.writes(evt)
        val partitionKey = shardProvider.get(evt, event)
        val data = Json.stringify(event).getBytes("UTF-8")
        val record = new PutRecordsRequestEntry().withPartitionKey(partitionKey).withData(ByteBuffer.wrap(data))

        markProducedEvent(config.streamName, event)

        // did the current batch reach one of the limitations?
        val newBytesSize = currentBytesSize + data.length
        if (currentSize == MaxBatchRecordsCount || newBytesSize > MaxBatchRecordsSizeBytes) {
          val newBatch = new util.ArrayList[PutRecordsRequestEntry](MaxBatchRecordsCount)
          batchedRecords += newBatch
          newBatch.add(record)
          (1L, data.length.toLong, newBatch)
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
            logger_.warn(errorMessage)
            response.getRecords.asScala.foreach { resultEntry =>
              if (Option(resultEntry.getErrorCode).isDefined || Option(resultEntry.getErrorMessage).isDefined)
                logger_.info(s"[FlowKinesisError] $resultEntry")
            }

            sys.error(errorMessage)
          } else {
            logger_.info(s"[FlowKinesisWarn] $failedRecordCount/${entries.size()} failed to be published. " +
              s"Retrying $attempts/$MaxRetries ...")

            val toRetries =
              entries.asScala.zip(response.getRecords.asScala)
                .collect { case (entry, res) if Option(res.getErrorCode).isDefined || Option(res.getErrorMessage).isDefined => entry }
            waitBeforeRetry()
            publishBatchRetries(toRetries.asJava, attempts + 1)
          }
        }

      case Failure(ex @ (_ : ProvisionedThroughputExceededException | _ : KMSThrottlingException | _ : NoHttpResponseException)) if attempts <= MaxRetries =>
        logger_.info(s"[FlowKinesisWarn] Exception thrown when publishing batch. Retrying $attempts/$MaxRetries ...", ex)
        waitBeforeRetry()
        publishBatchRetries(entries, attempts + 1)

      case Failure(ex) => throw ex
    }
  }

  // uniform 1s to 5s
  private def waitBeforeRetry(): Unit = Thread.sleep(1000L + Random.nextInt(4000).toLong)

  private def doPublishBatch(entries: util.List[PutRecordsRequestEntry]): PutRecordsResult = {
    val putRecordsRequest = new PutRecordsRequest().withStreamName(config.streamName).withRecords(entries)
    kinesisClient.putRecords(putRecordsRequest)
  }

  override def shutdown(): Unit = {
    kinesisClient.shutdown()
  }

  /**
    * Sets up the stream name in ec2, either an error or Unit
    **/
  @silent private[this] def setup(): Unit = {
    Try {
      kinesisClient.createStream(
        new CreateStreamRequest()
          .withStreamName(config.streamName)
          .withShardCount(numberShards)
      )
    }.map { _ =>
      // set retention to three days to recover from Flow service outages lasting longer than the default 24 hours
      // e.g. when a service comes back online it can recover the last 3 days of events from the Kinesis stream
      kinesisClient.increaseStreamRetentionPeriod(
        new IncreaseStreamRetentionPeriodRequest()
          .withStreamName(config.streamName)
          .withRetentionPeriodHours(72)
      )
    }.map { _ =>
      // createStream() immediately returns. we need to wait for the stream to go from CREATING -> ACTIVE.
      while (kinesisClient.describeStream(config.streamName).getStreamDescription.getStreamStatus == "CREATING") {
        logger_.withKeyValue("stream", config.streamName).info("waiting for stream to be created")
        Thread.sleep(1000)
      }
    }.recover {
      case NonFatal(ex) => {
        ex match {
          case _: ResourceInUseException => {
            // do nothing... already exists, ignore
          }

          case e: Throwable => {
            val msg = s"FlowKinesisError [${this.getClass.getName}] Stream[$config.streamName] could not be created. Error Message: ${e.getMessage}"
            logger_.warn(msg, e)
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

  // Let's really retry! - Yes it is arbitrary
  val MaxRetries = 128
}
