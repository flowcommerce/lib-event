package io.flow.event.v2

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, AWSStaticCredentialsProvider}
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}
import io.flow.event.Naming

case class StreamConfig(
  awsCredentials: AWSCredentials,
  appName: String,
  streamName: String,
  maxRecords: Int = 1000,   // number of records in each fetch
  idleTimeBetweenReadsInMillis: Int = 1000
) {

  val awSCredentialsProvider: AWSCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials)

  def kinesisClient: AmazonKinesis = {
    AmazonKinesisClientBuilder.standard().
      withCredentials(awSCredentialsProvider).
      withClientConfiguration(
        new ClientConfiguration()
          .withMaxErrorRetry(10)
          .withMaxConsecutiveRetriesBeforeThrottling(1)
          .withThrottledRetries(true)
          .withConnectionTTL(600000)
      ).
      build()
  }

  def dynamoTableName: String = {
    Naming.dynamoKinesisTableName(
      streamName = streamName,
      appName = appName
    )
  }

}

