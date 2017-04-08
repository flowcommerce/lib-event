package io.flow.event.v2

import java.nio.ByteBuffer

import com.amazonaws.ClientConfiguration
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import io.flow.event.Util
import play.api.libs.json.{JsValue, Json}
import com.amazonaws.services.kinesis.model.{CreateStreamRequest, PutRecordRequest, ResourceInUseException}
import play.api.Logger

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

case class KinesisProducer(
  config: StreamConfig,
  numberShards: Int,
  partitionKeyFieldName: String
) extends Producer {

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
