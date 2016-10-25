package io.flow.event


import io.flow.play.util.{FlowEnvironment, Random}
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model._
import play.api.libs.json.{JsValue, Json}
import play.api.Logger

import scala.reflect.runtime.universe._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import java.nio.ByteBuffer

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

  // Explicitly create streams with 4 shards to allow 20 concurrent connections
  private[this] val numberShards = 4
  private[this] val client = new AmazonKinesisClient(credentials)
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
          kinesisStreams.put(streamName, KinesisStream(client, streamName, numberShards))

        kinesisStreams.get(streamName).getOrElse(KinesisStream(client, streamName, numberShards))
      }
    }
  }

}

case class KinesisStream(
  client: AmazonKinesisClient,
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
    processMessages(f).recover {
      case e: Throwable => {
        Logger.error(s"FlowKinesisError Stream[$name] Error processing: $e")
      }
    }
  }

  /**
   * Publish Helper Functions
   **/
  def insertMessage(partitionKey: String, data: String) {
    try {
      client.putRecord(
        new PutRecordRequest()
          .withData(ByteBuffer.wrap(data.getBytes("UTF-8")))
          .withPartitionKey(partitionKey)
          .withStreamName(name)
      )
    } catch {
      case e: ResourceNotFoundException => {
        Logger.error(s"FlowKinesisError Stream[$name] does not exist. Error Message: ${e.getMessage}")
      }
      case e: Throwable => {
        Logger.error(s"FlowKinesisError Stream[$name] Could not insert message. Error Message: ${e.getMessage}")
      }
    }
  }

  /**
   * Consume Helper Functions
   **/
  def processMessages[T](
    f: JsValue => Unit
  )(implicit ec: ExecutionContext): Future[Unit] = {
    getShards.map { shards =>
      shards.map { shardId =>
        getShardIterator(shardId).map { shardIterator =>
          processShard(shardIterator, shardId, f)
        }
      }
    }
  }

  def processShard(
    shardIterator: String,
    shardId: String,
    f: JsValue => Unit
  )(implicit ec: ExecutionContext): Future[Unit] = {
    getMessagesForShardIterator(shardIterator, shardId).map { result =>
      result.messages.foreach{ msg =>
        f(Json.parse(msg.getBytes("UTF-8")))
      }

      result.nextShardIterator.map{nextShardIterator =>

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
  }

  /**
   * Sets up the stream name in ec2, either an error or Unit
   **/
  private[this] def setup(
    implicit ec: ExecutionContext
  ) {
    Try {
      client.createStream(
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
  )(implicit ec: ExecutionContext): Future[KinesisShardMessageSummary] = {
    Future {
      val request = new GetRecordsRequest()
        .withLimit(recordLimit)
        .withShardIterator(shardIterator)
      val result = client.getRecords(request)
      val millisBehindLatest = result.getMillisBehindLatest
      val records = result.getRecords

      val messages = records.asScala.map{record =>
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
  }

  /**
    * Since there is an AWS account limit allowing only 5 concurrent requests to describe streams,
    * cache the streamName -> shardIds.
    *
    * On service startup, the API will only be called at most once per stream name
    */
  def getShards(implicit ec: ExecutionContext): Future[Seq[String]] = {
    Future {
      if (!streamNameShardIds.contains(name)) {
        val shardIds = client.describeStream(
          new DescribeStreamRequest()
            .withStreamName(name)
        ).getStreamDescription.getShards.asScala.map(_.getShardId)

        streamNameShardIds += (name -> shardIds)

        shardIds
      } else {
        streamNameShardIds(name)
      }
    }
  }

  def getShardIterator(
    shardId: String
  )(implicit ec: ExecutionContext): Future[String] = {
    Future {
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

      client.getShardIterator(request).getShardIterator
    }
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
