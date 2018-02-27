package io.flow.event.v2

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, AWSStaticCredentialsProvider}
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}
import io.flow.event.Naming

trait StreamConfig {
  val appName: String
  val streamName: String
  val maxRecords: Int
  val idleTimeBetweenReadsInMillis: Int
  val awsCredentialsProvider: AWSCredentialsProvider

  def kinesisClient: AmazonKinesis

  def dynamoTableName: String = {
    Naming.dynamoKinesisTableName(
      streamName = streamName,
      appName = appName
    )
  }
}

case class DefaultStreamConfig(
  awsCredentials: AWSCredentials,
  appName: String,
  streamName: String,
  maxRecords: Int = 1000,   // number of records in each fetch
  idleTimeBetweenReadsInMillis: Int = 1000
) extends StreamConfig {

  override lazy val awsCredentialsProvider: AWSCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials)

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

case class NoCredentialsStreamConfig(
  appName: String,
  streamName: String,
  maxRecords: Int = 1000,   // number of records in each fetch
  idleTimeBetweenReadsInMillis: Int = 1000
) extends StreamConfig {

  override lazy val awsCredentialsProvider: AWSCredentialsProvider = sys.error("No credentials for NoCredentialsStreamConfig")

  override def kinesisClient: AmazonKinesis = {
    AmazonKinesisClientBuilder.standard().
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