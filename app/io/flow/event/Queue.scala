package io.flow.event


import java.io.{PrintWriter, StringWriter}

import io.flow.play.util.{FlowEnvironment, Random}
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model._
import play.api.libs.json.{JsValue, Json}
import play.api.Logger

import scala.reflect.runtime.universe._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}
import java.nio.ByteBuffer

import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient
import com.amazonaws.services.cloudwatch.model.{Dimension, MetricDatum, PutMetricDataRequest, StandardUnit}
import org.joda.time.DateTime

import collection.JavaConverters._

trait Queue {
  def stream[T: TypeTag](implicit ec: ExecutionContext): Stream
}

trait Stream {
  def publish(event: JsValue)
  def consume(f: JsValue => Unit)(implicit ec: ExecutionContext)
}

class KinesisQueue @javax.inject.Inject() (
  config: io.flow.play.util.Config
) extends Queue {

  private[this] val credentials = new BasicAWSCredentials(
    config.requiredString("aws.access.key"),
    config.requiredString("aws.secret.key")
  )

  private[this] val numberShards = 1
  private[this] val kinesisClient = new AmazonKinesisClient(credentials)
  private[this] val cloudWatchClient = new AmazonCloudWatchClient(credentials)

  var kinesisStreams: scala.collection.mutable.Map[String, KinesisStream] = scala.collection.mutable.Map[String, KinesisStream]()

  override def stream[T: TypeTag](implicit ec: ExecutionContext): Stream = {
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
          kinesisStreams.put(streamName, KinesisStream(cloudWatchClient, kinesisClient, streamName, numberShards))

        kinesisStreams.get(streamName).getOrElse(KinesisStream(cloudWatchClient, kinesisClient, streamName, numberShards))
      }
    }
  }

}

case class KinesisStream(
  cloudWatchClient: AmazonCloudWatchClient,
  kinesisClient: AmazonKinesisClient,
  name: String,
  numberShards: Int = 1
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
  private[this] var shardSequenceNumberMap = scala.collection.mutable.Map.empty[String,String]

  private[this] var streamNameShardIds = scala.collection.mutable.Map.empty[String,Seq[String]]

  setup

  def publish(event: JsValue) {
    val partitionKey = Random().alphaNumeric(30)
    val data = Json.stringify(event)
    insertMessage(partitionKey, data)
  }

  def consume(f: JsValue => Unit)(implicit ec: ExecutionContext) {
    processMessages(f)
  }

  /**
   * Publish Helper Functions
   **/
  def insertMessage(partitionKey: String, data: String) {
    try {
      kinesisClient.putRecord(
        new PutRecordRequest()
          .withData(ByteBuffer.wrap(data.getBytes("UTF-8")))
          .withPartitionKey(partitionKey)
          .withStreamName(name)
      )
    } catch {
      case e: ResourceNotFoundException => {
        Logger.error(s"FlowKinesisError Stream[$name] ResourceNotFoundException calling [putRecord]. Error Message: ${e.getMessage}")
      }
      case e: InvalidArgumentException => {
        Logger.error(s"FlowKinesisError Stream[$name] InvalidArgumentException calling [putRecord]. Error Message: ${e.getMessage}")
      }
      case e: ProvisionedThroughputExceededException => {
        Logger.error(s"FlowKinesisError Stream[$name] ProvisionedThroughputExceededException calling [putRecord]. Error Message: ${e.getMessage}")
      }
      case e: Throwable => {
        Logger.error(s"FlowKinesisError Stream[$name] Could not insert message. Error Message: ${e.getMessage}")
      }

      val sw = new StringWriter()
      e.printStackTrace(new PrintWriter(sw))
    }
  }

  /**
   * Consume Helper Functions
   **/
  def processMessages[T](
    f: JsValue => Unit
  )(implicit ec: ExecutionContext): Unit = {
    getShards.foreach { shardId =>
      val shardIterator = getShardIterator(shardId)
      processShard(shardIterator, shardId, f)
    }
  }

  def processShard(
    shardIterator: String,
    shardId: String,
    f: JsValue => Unit
  )(implicit ec: ExecutionContext): Unit = {
    val results = getMessagesForShardIterator(shardIterator, shardId)
    results.messages.foreach{ msg =>
      f(Json.parse(msg.getBytes("UTF-8")))
    }

    results.nextShardIterator.map{nextShardIterator =>

      /**
        * For best results, sleep for at least one second (1000 milliseconds) between calls to getRecords to avoid exceeding the limit on getRecords frequency.
        *
        * Documentation:
        * https://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-sdk.html?shortFooter=true#kinesis-using-sdk-java-get-data-getrecords
        */
      try {
        Thread.sleep(1000)
      } catch {
        case e: InterruptedException => sys.error(s"Error occurred while sleeping between calls to getRecords.  Error was: $e")
      }

      processShard(nextShardIterator, shardId, f)
    }
  }

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
            Left(s"Stream $name could not be created: ${e.getMessage}")
          }
        }
      }
    }
  }

  def getMessagesForShardIterator(
    shardIterator: String,
    shardId: String
  )(implicit ec: ExecutionContext): KinesisShardMessageSummary = {
    val request = new GetRecordsRequest()
      .withLimit(recordLimit)
      .withShardIterator(shardIterator)

    val result = getRecords(request)

    val millisBehindLatest = result.getMillisBehindLatest
    val records = result.getRecords

    val messages = records.asScala.map{record =>

      /**
        * For each unique stream/shard, put a metric to AWS CloudWatch indicating the stream latency
        * Stream Latency = Current Time - Record Approximate Arrival Timestamp (approx. arrival in the stream)
        *
        */
      //putStreamLatencyMetric(name, shardId, record.getApproximateArrivalTimestamp.getTime)

      shardSequenceNumberMap += (shardId -> record.getSequenceNumber)
      val buffer = record.getData
      val bytes = Array.fill[Byte](buffer.remaining)(0)
      buffer.get(bytes)
      new String(bytes, "UTF-8")
    }

    val nextShardIterator = (millisBehindLatest == 0) match {
      case true => None
      case false => Some(result.getNextShardIterator)
    }

    KinesisShardMessageSummary(messages, nextShardIterator)
  }

  def getRecords(request: GetRecordsRequest): GetRecordsResult = {
    Try {
      kinesisClient.getRecords(request)
    } match {
      case Success(results) =>
        results
      case Failure(ex) => {

        val sw = new StringWriter()
        ex.printStackTrace(new PrintWriter(sw))

        ex match {
          case e: ResourceNotFoundException =>
            val msg = s"FlowKinesisError Stream[$name] ResourceNotFoundException calling [getRecords]. Error Message: ${e.getMessage}"
            Logger.error(msg)
            throw new Exception(msg, ex)
          case e: InvalidArgumentException =>
            val msg = s"FlowKinesisError Stream[$name] ResourceNotFoundException calling [getRecords]. Error Message: ${e.getMessage}"
            Logger.error(msg)
            throw new Exception(msg, ex)
          case e: ProvisionedThroughputExceededException =>
            val msg = s"FlowKinesisError Stream[$name] ProvisionedThroughputExceededException calling [getRecords]. Error Message: ${e.getMessage}"
            Logger.error(msg)
            throw new Exception(msg, ex)
          case e: ExpiredIteratorException =>
            val msg = s"FlowKinesisError Stream[$name] ExpiredIteratorException calling [getRecords]. Error Message: ${e.getMessage}"
            Logger.error(msg)
            throw new Exception(msg, ex)
          case ex: Throwable => {
            val msg = s"Failed get records.  Error was: ${ex.getMessage}"
            Logger.error(msg)
            throw new Exception(msg, ex)
          }
        }
      }
    }
  }

  /**
    * Since there is an AWS account limit allowing only 5 concurrent requests to describe streams,
    * cache the streamName -> shardIds.
    *
    * On service startup, the API will only be called at most once per stream name
    */
  def getShards(implicit ec: ExecutionContext): Seq[String] = {
  if (!streamNameShardIds.contains(name)) {

      Try {
        kinesisClient.describeStream(
          new DescribeStreamRequest()
            .withStreamName(name)
        )
      } match {
        case Success(results) =>
          val shardIds = results.getStreamDescription.getShards.asScala.map(_.getShardId)

          streamNameShardIds += (name -> shardIds)

          Logger.info(s"Stream Name -> Shard Ids mapping for stream [$name] is [$streamNameShardIds]")
          shardIds
        case Failure(ex) => {
          case e: ResourceNotFoundException =>
            val msg = s"FlowKinesisError Stream[$name] ResourceNotFoundException calling [getShards]. Error Message: ${e.getMessage}"
            Logger.error(msg)
          case e: LimitExceededException =>
            val msg = s"FlowKinesisError Stream[$name] LimitExceededException calling [getShards]. Error Message: ${e.getMessage}"
            Logger.error(msg)
          case ex: Throwable => {
            val msg = s"Failed get records.  Error was: ${ex.getMessage}"
            Logger.error(msg)
          }

          val sw = new StringWriter()
          ex.printStackTrace(new PrintWriter(sw))
        }
      }
    } else {
      streamNameShardIds(name)
    }
  }

  def getShardIterator(
    shardId: String
  )(implicit ec: ExecutionContext): String = {
    val baseRequest = new GetShardIteratorRequest()
      .withShardId(shardId)
      .withStreamName(name)

    val request = shardSequenceNumberMap.contains(shardId) match {
      case true => {
        baseRequest
        .withStartingSequenceNumber(shardSequenceNumberMap(shardId))
        .withShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
      }
      case false => {
        baseRequest.withShardIteratorType(ShardIteratorType.TRIM_HORIZON)
      }
    }

    Try {
      kinesisClient.getShardIterator(request)
    } match {
      case Success(results) =>
        results.getShardIterator

      case Failure(ex) => {
        case e: ResourceNotFoundException =>
          val msg = s"FlowKinesisError Stream[$name] ResourceNotFoundException calling [getShardIterator]. Error Message: ${e.getMessage}"
          Logger.error(msg)
        case e: InvalidArgumentException =>
          val msg = s"FlowKinesisError Stream[$name] InvalidArgumentException calling [getShardIterator]. Error Message: ${e.getMessage}"
          Logger.error(msg)
        case ex: Throwable => {
          val msg = s"Failed get records.  Error was: ${ex.getMessage}"
          Logger.error(msg)
        }

        val sw = new StringWriter()
        ex.printStackTrace(new PrintWriter(sw))
      }
    }

  }

  def putStreamLatencyMetric(streamName: String, shardId: String, recordArrivalTime: Long) = {
    cloudWatchClient.putMetricData(
      new PutMetricDataRequest()
        .withNamespace("Flow")
        .withMetricData(
          new MetricDatum()
            .withMetricName("StreamLatency")
            .withDimensions(
              new Dimension()
                .withName("StreamName")
                .withValue(name),
              new Dimension()
                .withName("ShardId")
                .withValue(shardId)
            )
            .withValue(DateTime.now().minus(recordArrivalTime).getMillis.toDouble)
            .withUnit(StandardUnit.Milliseconds)
        )
    )
  }

}

@javax.inject.Singleton
class MockQueue extends Queue {

  var mockStreams: scala.collection.mutable.Map[String, MockStream] = scala.collection.mutable.Map[String, MockStream]()

  override def stream[T: TypeTag](implicit ec: ExecutionContext): Stream = {
    val name = typeOf[T].toString

    if (!mockStreams.contains(name))
      mockStreams.put(name, new MockStream())

    mockStreams.get(name).get
  }
}

@javax.inject.Singleton
class MockStream extends Stream {

  private var events = scala.collection.mutable.ListBuffer[JsValue]()

  def publish(event: JsValue) {
    events += event
  }

  def consume(f: JsValue => Unit)(implicit ec: ExecutionContext) {
    events.toList match {
      case Nil => {
        // no events
      }

      case one :: rest => {
        events.trimStart(1)
        f(one)
      }
    }
  }

}
