package io.flow.event

import io.flow.play.util.FlowEnvironment

import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials}
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model._

import play.api.libs.json.{JsValue,Json}
import play.api.Logger

import scala.reflect.runtime.universe._
import scala.concurrent.{Await,Future,ExecutionContext}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

import java.nio.ByteBuffer
import collection.JavaConverters._

trait Queue {
  def stream[T: TypeTag](implicit ec: ExecutionContext): Stream
}

trait Stream {
  def publish(event: JsValue)(implicit ec: ExecutionContext)
  def consume(f: JsValue => Unit)(implicit ec: ExecutionContext)
}

class KinesisQueue @javax.inject.Inject() (
  config: io.flow.play.util.Config
) extends Queue {

  private[this] val credentials = new BasicAWSCredentials(
    config.requiredString("aws.access.key"),
    config.requiredString("aws.secret.key")
  )

  private[this] val client = new AmazonKinesisClient(credentials)

  private[this] val ApidocClass = "^io\\.flow\\.([a-z]+)(\\.([a-z]+))*\\.(v\\d+)\\.models\\.(\\w+)$".r

  def toSnakeCase(name: String): String = {
    name.replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2").replaceAll("([a-z\\d])([A-Z])", "$1_$2").toLowerCase
  }

  override def stream[T: TypeTag](implicit ec: ExecutionContext): Stream = typeOf[T].toString match {
    case ApidocClass(service, version, className) => {
      val snakeClassName = toSnakeCase(className)
      val name = s"${FlowEnvironment.Current}.$service.$version.$snakeClassName.json"
      KinesisStream(client, name)
    }

    case "Any" => {
      sys.error(s"In order to consume events, you must annotate the type you are expecting as this is used to build the stream. Type should be something like io.flow.user.v0.models.Event")
    }

    case other => {
      sys.error(s"Could not parse stream name of type[$other]. Expected something like io.flow.user.v0.models.Event")
    }
  }

}

case class KinesisStream(
  client: AmazonKinesisClient,
  name: String
) (
  implicit ec: ExecutionContext
) extends Stream {

  private[this] val recordLimit = 1000
  private[this] val shardCreateCount = 1
  private[this] var shardSequenceNumberMap = scala.collection.mutable.Map.empty[String,String]

  setup

  def publish(event: JsValue)(implicit ec: ExecutionContext) {
    val partitionKey = name // TODO: Figure out what this should be based on available TypeRef vals
    val data = Json.stringify(event)
    insertMessage(partitionKey, data)
  }

  def consume(f: JsValue => Unit)(implicit ec: ExecutionContext) {
    processMessages(f).recover {
      case e: Throwable => {
        sys.error(s"Error processing stream $name: $e")
      }
    }
  }

  /**
   * Publish Helper Functions
   **/
  def insertMessage(
    partitionKey: String,
    data: String
  )(implicit ec: ExecutionContext): Future[Unit] = {
    Future {
      try {
        client.putRecord(
          new PutRecordRequest()
          .withData(ByteBuffer.wrap(data.getBytes))
          .withPartitionKey(partitionKey)
          .withStreamName(name)
        )
      } catch {
        case e: ResourceNotFoundException => {
          sys.error(s"Stream $name does not exist. Error Message: ${e.getMessage}")
        }
        case e: Throwable => {
          sys.error(s"Could not insert message to stream $name. Error Message: ${e.getMessage}")
        }
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
        f(Json.parse(msg))
      }

      result.nextShardIterator.map{nextShardIterator =>
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
          .withShardCount(shardCreateCount)
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
      val records = result.getRecords

      val messages = records.asScala.map{record =>
        shardSequenceNumberMap += (shardId -> record.getSequenceNumber)
        val buffer = record.getData
        val bytes = Array.fill[Byte](buffer.remaining)(0)
        buffer.get(bytes)
        new String(bytes)
      }

      val nextShardIterator = (records.size == recordLimit) match {
        case true => Some(result.getNextShardIterator)
        case false => None
      }

      KinesisShardMessageSummary(messages, nextShardIterator)
    }
  }

  def getShards(implicit ec: ExecutionContext): Future[Seq[String]] = {
    Future {
      client.describeStream(
        new DescribeStreamRequest()
        .withStreamName(name)
      ).getStreamDescription.getShards.asScala.map(_.getShardId)
    }
  }

  def getShardIterator(
    shardId: String
  )(implicit ec: ExecutionContext): Future[String] = {
    Future {
      val baseRequest = new GetShardIteratorRequest()
        .withShardId(shardId)
        .withStreamName(name)

      // for sanity
      Logger.info(s"Stream: $name, Shard Sequence Number Map: $shardSequenceNumberMap")

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

class MockQueue extends Queue {

  override def stream[T: TypeTag](implicit ec: ExecutionContext): Stream = {
    new MockStream[T]()
  }

}

class MockStream[T: TypeTag] extends Stream {

  private var events = scala.collection.mutable.ListBuffer[JsValue]()

  def publish(event: JsValue)(implicit ec: ExecutionContext) {
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
