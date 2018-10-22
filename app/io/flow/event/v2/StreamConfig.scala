package io.flow.event.v2

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}
import io.flow.util.Naming
import scala.reflect.runtime.universe._

trait StreamConfig {
  val appName: String
  val streamName: String
  val maxRecords: Int
  val idleTimeBetweenReadsInMillis: Int
  val awsCredentialsProvider: AWSCredentialsProvider
  val eventClass: Type

  def kinesisClient: AmazonKinesis

  def dynamoTableName: String = {
    Naming.dynamoKinesisTableName(
      streamName = streamName,
      appName = appName
    )
  }
}

case class DefaultStreamConfig(
  awsCredentialsProvider: AWSCreds,
  appName: String,
  streamName: String,
  maxRecords: Int = 1000,   // number of records in each fetch
  idleTimeBetweenReadsInMillis: Int = 1000,
  eventClass: Type
) extends StreamConfig {

  override def kinesisClient: AmazonKinesis = {
    AmazonKinesisClientBuilder.standard().
      withCredentials(awsCredentialsProvider).
      withClientConfiguration(
        new ClientConfiguration()
          .withMaxErrorRetry(10)
          .withMaxConsecutiveRetriesBeforeThrottling(1)
          .withThrottledRetries(true)
          .withConnectionTTL(600000)
      ).
      build()
  }
}
