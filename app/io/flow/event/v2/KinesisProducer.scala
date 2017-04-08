package io.flow.event.v2

import java.nio.ByteBuffer

import com.amazonaws.ClientConfiguration
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import io.flow.event.Util
import io.flow.play.util.Config
import play.api.libs.json.{JsValue, Json}
import com.amazonaws.services.kinesis.model.{CreateStreamRequest, PutRecordRequest, ResourceInUseException}
import play.api.Logger

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

/**
  * @param partitionKeyFieldName The partition key field name must exist
  *                              as a key in the JSON object published.
  */
case class KinesisProducer(
  config: Config,
  streamName: String,
  numberShards: Int = 1,
  partitionKeyFieldName: String = "id"
) extends Producer {

  private[this] val clientConfig =
    new ClientConfiguration()
      .withMaxErrorRetry(5)
      .withThrottledRetries(true)
      .withConnectionTTL(60000)

  private[this] val kinesisClient = AmazonKinesisClientBuilder.standard().
    withCredentials(FlowConfigAWSCredentialsProvider(config)).
    withClientConfiguration(clientConfig).
    build()

  setup()

  /**
    * Sets up the stream name in ec2, either an error or Unit
    **/
  private[this] def setup() {
    Try {
      kinesisClient.createStream(
        new CreateStreamRequest()
          .withStreamName(streamName)
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
            Logger.warn(s"FlowKinesisError [${this.getClass.getName}] Stream[$streamName] could not be created. Error Message: ${e.getMessage}")
            e.printStackTrace(System.err)
            sys.error(s"FlowKinesisError [${this.getClass.getName}] Stream[$streamName] could not be created. Error Message: ${e.getMessage}")
          }
        }
      }
    }
  }

  override def publish(event: JsValue)(implicit ec: ExecutionContext) {
    val partitionKey = Util.mustParseString(event, partitionKeyFieldName)
    val bytes = Json.stringify(event).getBytes("UTF-8")

    kinesisClient.putRecord(
      new PutRecordRequest()
        .withData(ByteBuffer.wrap(bytes))
        .withPartitionKey(partitionKey)
        .withStreamName(streamName)
    )
  }
}
