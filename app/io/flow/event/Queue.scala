package io.flow.event


import io.flow.play.util.{FlowEnvironment, Random}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}
import com.amazonaws.services.kinesis.model._
import play.api.libs.json.{JsValue, Json}
import play.api.Logger

import scala.reflect.runtime.universe._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}
import java.nio.ByteBuffer
import java.util.UUID

import com.amazonaws.ClientConfiguration
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat.dateTimeParser

import collection.JavaConverters._

trait Queue {
  def stream[T: TypeTag](sequenceNumberProvider: SequenceNumberProvider)(implicit ec: ExecutionContext): Stream
}

trait Stream {
  def publish(event: JsValue)
  def consume(f: Record => Unit)(implicit ec: ExecutionContext)
}

object Record {

  def fromByteArray(arrivalTimestamp: DateTime, value: Array[Byte], streamName: String, shardId: String, sequenceNumber: String): Record = {
    fromJsValue(arrivalTimestamp, Json.parse(value), streamName, shardId, sequenceNumber)
  }

  def fromJsValue(arrivalTimestamp: DateTime, js: JsValue, streamName: String, shardId: String, sequenceNumber: String): Record = {
    Record(
      eventId = Util.mustParseString(js, "event_id"),
      timestamp = dateTimeParser.parseDateTime(
        Util.mustParseString(js, "timestamp")
      ),
      js = js,
      arrivalTimestamp = arrivalTimestamp,
      streamName = streamName,
      shardId = shardId,
      sequenceNumber = sequenceNumber
    )
  }
  
}


case class Record(
  eventId: String,
  timestamp: DateTime,
  arrivalTimestamp: DateTime,
  streamName: String,
  shardId: String,
  sequenceNumber: String,
  js: JsValue
)

case class Message(
  message: String,
  arrivalTimestamp: DateTime,
  streamName: String,
  shardId: String,
  sequenceNumber: String
)

object QueueConstants {

  val ShardIteratorExpirationTimeMinutes = 4

}

/**
  * Wraps a kinesis shard iterator, adding an expiration
  */
case class ShardIterator(
  shardIterator: String,
  expiresAt: DateTime = DateTime.now.plusMinutes(QueueConstants.ShardIteratorExpirationTimeMinutes)
) {

  def isExpired: Boolean = expiresAt.isBeforeNow

}

class KinesisQueue @javax.inject.Inject() (
  config: io.flow.play.util.Config
) extends Queue {

  private[this] val credentials = new BasicAWSCredentials(
    config.requiredString("aws.access.key"),
    config.requiredString("aws.secret.key")
  )

  private[this] val clientConfig =
    new ClientConfiguration()
      .withMaxErrorRetry(5)
      .withThrottledRetries(true)
      .withConnectionTTL(60000)

  private[this] val numberShards = 1
  private[this] val kinesisClient = AmazonKinesisClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).withClientConfiguration(clientConfig).build()

  var kinesisStreams: scala.collection.mutable.Map[String, KinesisStream] = scala.collection.mutable.Map[String, KinesisStream]()

  override def stream[T: TypeTag](sequenceNumberProvider: SequenceNumberProvider)(implicit ec: ExecutionContext): Stream = {
    val name = typeOf[T].toString

    StreamNames(FlowEnvironment.Current).json(name) match {
      case None => {
        name match {
          case "Any" => {
            sys.error(s"FlowKinesisError Stream[$name] In order to consume events, you must annotate the type you are expecting as this is used to build the stream. Type should be something like io.flow.user.v0.unions.SomeEvent")
          }
          case _ => {
            sys.error(s"FlowKinesisError Stream[$name] Could not parse stream name. Expected something like io.flow.user.v0.unions.SomeEvent")
          }
        }
      }
      case Some(streamName) => {
        if (!kinesisStreams.contains(streamName))
          kinesisStreams.put(streamName, KinesisStream(kinesisClient, streamName, numberShards, sequenceNumberProvider))

        kinesisStreams.getOrElse(streamName, KinesisStream(kinesisClient, streamName, numberShards, sequenceNumberProvider))
      }
    }
  }

}

case class KinesisStream(
  kinesisClient: AmazonKinesis,
  name: String,
  numberShards: Int = 1,
  sequenceNumberProvider: SequenceNumberProvider
) (
  implicit ec: ExecutionContext
) extends Stream {

  /**
    * "...up to a maximum total data read rate of 2 MB per second.
    * Note that each read (GetRecords call) gets a batch of records.
    * The size of the data returned by GetRecords varies depending on the utilization of the shard.
    * The maximum size of data that GetRecords can return is 10 MB.
    * If a call returns that limit, subsequent calls made within the next 5 seconds throw ProvisionedThroughputExceededException."
    *
    * Let's try to grab the optimal amount -
    * The below recordLimit is a general guess given one of Flow's larger messages (~2300 bytes).
    * 2300 * 750 = 1.72 MB
    *
    * Documentation:
    * https://docs.aws.amazon.com/streams/latest/dev/troubleshooting-consumers.html?shortFooter=true
    */
  private[this] val recordLimit = 750
  private[this] var shardIteratorMap = scala.collection.mutable.Map.empty[String, ShardIterator]
  private[this] var streamNameShardIds = scala.collection.mutable.Map.empty[String, Seq[String]]

  setup

  /**
    * Sets up the stream name in ec2, either an error or Unit
    **/
  private[this] def setup(
    implicit ec: ExecutionContext
  ) {
    Try {
      kinesisClient.createStream(
        new CreateStreamRequest()
          .withStreamName(name)
          .withShardCount(numberShards)
      )
    } match {
      case Success(_) => {
        // All good
      }

      case Failure(ex) => {
        ex match {
          case e: ResourceInUseException => {
            // do nothing... already exists, ignore
            Right(())
          }

          case e: Throwable => {
            e.printStackTrace(System.err)
            Left(s"FlowKinesisError Stream[$name] could not be created calling [io.flow.event.setup]. Error Message: ${e.getMessage}")
          }
        }
      }
    }
  }

  def publish(event: JsValue) {
    val partitionKey = "1"
    val data = Json.stringify(event)
    insertMessage(partitionKey, data)
  }

  def consume(f: Record => Unit)(implicit ec: ExecutionContext) {
    processMessages(f)
  }

  /**
    * Publish Helper Functions
    **/
  def insertMessage(partitionKey: String, data: String) {
    withErrorHandler("insertMessage") {
      kinesisClient.putRecord(
        new PutRecordRequest()
          .withData(ByteBuffer.wrap(data.getBytes("UTF-8")))
          .withPartitionKey(partitionKey)
          .withStreamName(name)
      )
    }
  }

  /**
    * Consume Helper Functions
    **/
  def processMessages[T](
    f: Record => Unit
  )(implicit ec: ExecutionContext): Unit = {
    getShards.foreach { shardId =>
      val shardIterator = getShardIterator(shardId)
      processShard(shardIterator, shardId, f)
    }
  }

  /**
    * Since there is an AWS account limit allowing only 5 concurrent requests to describe streams,
    * cache the streamName -> shardIds.
    *
    * On service startup, the API will only be called at most once per stream name
    */
  def getShards(implicit ec: ExecutionContext): Seq[String] = {
    // TODO: Add an expiration here
    streamNameShardIds.get(name).getOrElse {
      withErrorHandler("getShards") {
        val results = kinesisClient.describeStream(
          new DescribeStreamRequest()
            .withStreamName(name)
        )

        val shardIds = results.getStreamDescription.getShards.asScala.map(_.getShardId)

        streamNameShardIds += (name -> shardIds)

        Logger.info(s"Stream Name -> Shard Ids mapping for stream [$name] is [$streamNameShardIds]")
        shardIds
      }
    }
  }

  /**
    * If cache does not contain a key for the shardId, it is because the consuming application has just started.
    * If consuming application has just restarted, ShardIteratorType.TRIM_HORIZON to read from the oldest record in the shard.
    * Else,
    * If cached shard iterator is older than 3 minutes, refresh the cache since it expires after 5 minutes
    * Otherwise, use cached shard iterator
    *
    * On service startup, the API will only be called at most once per shard per stream
    */
  def getShardIterator(
    shardId: String
  )(implicit ec: ExecutionContext): String = {
    val iterator = shardIteratorMap.get(shardId) match {
      case None => {
        sequenceNumberProvider.current(name, shardId) match {
          case Some(sequenceNumber) =>
            // sequence number for stream name/shard retrieved from provider
            // store it in local memory
            val snapshot = Snapshot(name, shardId, sequenceNumber)
            LocalSnapshotManager(name, shardId).putSnapshot(snapshot)

            refreshShardIterator(shardId, ShardIteratorType.AFTER_SEQUENCE_NUMBER)

          case None =>
            refreshShardIterator(shardId, ShardIteratorType.TRIM_HORIZON)
        }
      }

      case Some(cachedIterator) => {
        if (cachedIterator.isExpired) {
          refreshShardIterator(shardId, ShardIteratorType.AFTER_SEQUENCE_NUMBER)
        } else {
          cachedIterator
        }
      }
    }

    iterator.shardIterator
  }

  private[this] def getShardIterator(shardId: String, shardIteratorType: ShardIteratorType): ShardIterator = {

    val baseRequest = new GetShardIteratorRequest()
      .withShardId(shardId)
      .withStreamName(name)

    val defaultShardIteratorType = ShardIteratorType.TRIM_HORIZON

    val request = shardIteratorType match {
      case ShardIteratorType.TRIM_HORIZON =>
        baseRequest
          .withShardIteratorType(shardIteratorType)

      case ShardIteratorType.AFTER_SEQUENCE_NUMBER =>
        LocalSnapshotManager(name, shardId).latestSnapshot match {
          case None =>
            Logger.info(s"No starting sequence number exists for stream name [$name] and shardId [$shardId].  Defaulting to [ShardIteratorType.TRIM_HORIZON]")
            baseRequest
              .withShardIteratorType(defaultShardIteratorType)
          case Some(snapshot) =>
            baseRequest
              .withStartingSequenceNumber(snapshot.sequenceNumber)
              .withShardIteratorType(shardIteratorType)
        }

      case other =>
        Logger.error(s"Unsupported ShardIteratorType [${other.toString}].  Please specify either [${ShardIteratorType.TRIM_HORIZON.toString}, ${ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString}].  Defaulting to [${defaultShardIteratorType.toString}]")
        baseRequest
          .withShardIteratorType(defaultShardIteratorType)
    }

    val shardIterator = withErrorHandler("getShardIterator") {
      kinesisClient.getShardIterator(request).getShardIterator
    }
    ShardIterator(shardIterator)
  }

  private[this] def refreshShardIterator(shardId: String, shardIteratorType: ShardIteratorType): ShardIterator = {
    val iterator = getShardIterator(shardId, shardIteratorType)

    Logger.info(s"Refreshing shard iterator for stream [$name] shardId [$shardId]: ${iterator.shardIterator} expires at [${iterator.expiresAt}]")
    shardIteratorMap += (shardId -> iterator)

    iterator
  }


  def processShard(
    shardIterator: String,
    shardId: String,
    f: Record => Unit
  )(implicit ec: ExecutionContext): Unit = {
    val results = getMessagesForShardIterator(shardIterator, shardId)
    results.messages.foreach { msg =>
      val data = Record.fromByteArray(
        arrivalTimestamp = new DateTime(msg.arrivalTimestamp),
        value = msg.message.getBytes("UTF-8"),
        streamName = name,
        shardId = shardId,
        sequenceNumber = msg.sequenceNumber
      )

      f(data)
    }

    results.nextShardIterator.map { _ =>

      /**
        * For best results, sleep for at least one second (1000 milliseconds) between calls to getRecords to avoid exceeding the limit on getRecords frequency.
        *
        * Documentation:
        * https://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-sdk.html?shortFooter=true#kinesis-using-sdk-java-get-data-getrecords
        */
      try {
        Thread.sleep(1500)
      } catch {
        case e: InterruptedException => sys.error(s"Error occurred while sleeping between calls to getRecords.  Error was: $e")
      }

      // to ensure shard iterator does not expire while processing messages, check it - uses either cached shard iterator or gets one from Kinesis
      val i = getShardIterator(shardId)

      processShard(i, shardId, f)
    }
  }

  def getMessagesForShardIterator(
    shardIterator: String,
    shardId: String
  )(implicit ec: ExecutionContext): KinesisShardMessageSummary = {
    val request = new GetRecordsRequest()
      .withLimit(recordLimit)
      .withShardIterator(shardIterator)

    val result = withErrorHandler("getRecords") {
      kinesisClient.getRecords(request)
    }

    val millisBehindLatest = result.getMillisBehindLatest
    val records = result.getRecords

    val messages = records.asScala.map { record =>
      // record latest sequence number for stream name/shard in memory
      LocalSnapshotManager(name, shardId).putSnapshot(Snapshot(name, shardId, record.getSequenceNumber))

      val buffer = record.getData
      val bytes = Array.fill[Byte](buffer.remaining)(0)
      buffer.get(bytes)

      Message(
        message = new String(bytes, "UTF-8"),
        arrivalTimestamp = new DateTime(record.getApproximateArrivalTimestamp),
        streamName = name,
        shardId = shardId,
        sequenceNumber = record.getSequenceNumber
      )
    }

    /**
      * Must update shard iterator cache, even if we are up to date, otherwise, shard iterator will remain
      * at current position and we'll continue to loop and consume the same events.
      */
    shardIteratorMap += (shardId -> ShardIterator(shardIterator = result.getNextShardIterator))

    val nextShardIterator = (millisBehindLatest == 0) match {
      case true => None
      case false => Some(result.getNextShardIterator)
    }

    KinesisShardMessageSummary(messages, nextShardIterator)
  }

  private[this] def withErrorHandler[T](
    methodName: String
  )(
    f: => T
  ) = {
    Try {
      f
    } match {
      case Success(results) =>
        results
      case Failure(ex) => {
        ex.printStackTrace(System.err)
        ex match {
          case e: ResourceNotFoundException =>
            val msg = s"FlowKinesisError Stream[$name] ResourceNotFoundException calling [io.flow.event.$methodName]. Error Message: ${e.getMessage}"
            Logger.error(msg)
            throw new Exception(msg, ex)
          case e: InvalidArgumentException =>
            val msg = s"FlowKinesisError Stream[$name] InvalidArgumentException calling [io.flow.event.$methodName]. Error Message: ${e.getMessage}"
            Logger.error(msg)
            throw new Exception(msg, ex)
          case e: LimitExceededException =>
            val msg = s"FlowKinesisError Stream[$name] LimitExceededException calling [io.flow.event.$methodName]. Error Message: ${e.getMessage}"
            Logger.error(msg)
            throw new Exception(msg, ex)

          /**
            * Log ProvisionedThroughputExceededException as a warning to avoid noise as we expect to receive this often
            */
          case e: ProvisionedThroughputExceededException =>
            val msg = s"FlowKinesisWarning Stream[$name] ProvisionedThroughputExceededException calling [io.flow.event.$methodName]. Error Message: ${e.getMessage}"
            Logger.warn(msg)
            throw new Exception(msg, ex)
          case e: ExpiredIteratorException =>
            val msg = s"FlowKinesisError Stream[$name] ExpiredIteratorException calling [io.flow.event.$methodName]. Error Message: ${e.getMessage}"
            Logger.error(msg)
            throw new Exception(msg, ex)
          case ex: Throwable => {
            val msg = s"FlowKinesisError Stream [$name] Failed in [io.flow.event.$methodName].  Error was: ${ex.getMessage}"
            Logger.error(msg)
            throw new Exception(msg, ex)
          }
        }
      }
    }
  }

}

@javax.inject.Singleton
class MockQueue extends Queue {

  var mockStreams: scala.collection.mutable.Map[String, MockStream] = scala.collection.mutable.Map[String, MockStream]()

  override def stream[T: TypeTag](sequenceNumberProvider: SequenceNumberProvider)(implicit ec: ExecutionContext): Stream = {
    val name = typeOf[T].toString

    if (!mockStreams.contains(name))
      mockStreams.put(name, new MockStream())

    mockStreams(name)
  }
}

@javax.inject.Singleton
class MockStream extends Stream {

  private[this] var events = scala.collection.mutable.ListBuffer[JsValue]()

  def publish(event: JsValue) {
    events += event
  }

  def consume(f: Record => Unit)(implicit ec: ExecutionContext) {
    events.toList match {
      case Nil => {
        // no events
      }

      case one :: rest => {
        events.trimStart(1)

        val data = Record.fromJsValue(
          arrivalTimestamp = new DateTime(),
          streamName = new Random().alpha(20),
          shardId = UUID.randomUUID.toString,
          sequenceNumber = UUID.randomUUID.toString,
          js = one
        )
        f(data)
      }
    }
  }

}
