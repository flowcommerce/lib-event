package io.flow.event.v2

import java.net.InetAddress
import java.util.UUID

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration}
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}
import io.flow.util.{FlowEnvironment, Naming}

import scala.reflect.runtime.universe._

trait StreamConfig {
  def appName: String
  def streamName: String
  def maxRecords: Option[Int]
  def idleMillisBetweenCalls: Option[Long]
  def idleTimeBetweenReadsInMillis: Option[Long]
  def maxLeasesForWorker: Option[Int]
  def maxLeasesToStealAtOneTime: Option[Int]
  def eventClass: Type
  def endpoints: AWSEndpoints

  def kinesisClient: AmazonKinesis

  def dynamoTableName: String = {
    Naming.dynamoKinesisTableName(
      streamName = streamName,
      appName = appName
    )
  }

  lazy val workerId: String = Seq(
    appName,
    InetAddress.getLocalHost.getCanonicalHostName,
    UUID.randomUUID.toString
  ).mkString(":")

  def toKclConfig(creds: AWSCredentialsProviderChain): KinesisClientLibConfiguration = {
    val dynamoCapacity = {
      FlowEnvironment.Current match {
        case FlowEnvironment.Production => 10 // 10 is the default value in the AWS SDK
        case FlowEnvironment.Development | FlowEnvironment.Workstation => 1
      }
    }

    val kclConf = new KinesisClientLibConfiguration(
      appName,
      streamName,
      creds,
      workerId
    ).withTableName(dynamoTableName)
      .withInitialLeaseTableReadCapacity(dynamoCapacity)
      .withInitialLeaseTableWriteCapacity(dynamoCapacity)
      .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
      .withCleanupLeasesUponShardCompletion(true)
      .withIdleMillisBetweenCalls(idleMillisBetweenCalls.getOrElse(1500L))
      .withIdleTimeBetweenReadsInMillis(idleTimeBetweenReadsInMillis.getOrElse(KinesisClientLibConfiguration.DEFAULT_IDLETIME_BETWEEN_READS_MILLIS))
      .withMaxRecords(maxRecords.getOrElse(1000))
      .withMaxLeasesForWorker(maxLeasesForWorker.getOrElse(KinesisClientLibConfiguration.DEFAULT_MAX_LEASES_FOR_WORKER))
      .withMaxLeasesToStealAtOneTime(maxLeasesToStealAtOneTime.getOrElse(KinesisClientLibConfiguration.DEFAULT_MAX_LEASES_TO_STEAL_AT_ONE_TIME))
      .withMetricsLevel(MetricsLevel.NONE)
      .withFailoverTimeMillis(30000) // See https://github.com/awslabs/amazon-kinesis-connectors/issues/10

    endpoints.kinesis.foreach { ep =>
      kclConf.withKinesisEndpoint(ep)
    }

    endpoints.dynamodb.foreach { ep =>
      kclConf.withDynamoDBEndpoint(ep)
    }

    kclConf
  }
}

case class DefaultStreamConfig(
  awsCredentialsProvider: AWSCreds,
  appName: String,
  streamName: String,
  maxRecords: Option[Int],   // number of records in each fetch
  idleMillisBetweenCalls: Option[Long],
  idleTimeBetweenReadsInMillis: Option[Long],
  maxLeasesForWorker: Option[Int],
  maxLeasesToStealAtOneTime: Option[Int],
  eventClass: Type,
  endpoints: AWSEndpoints,
) extends StreamConfig {

  override lazy val kinesisClient: AmazonKinesis = {
    val kclb = AmazonKinesisClientBuilder.standard().
      withCredentials(awsCredentialsProvider).
      withClientConfiguration(
        new ClientConfiguration()
          .withMaxErrorRetry(10)
          .withMaxConsecutiveRetriesBeforeThrottling(1)
          .withThrottledRetries(true)
          .withConnectionTTL(600000)
      )

    endpoints.kinesis.foreach { ep =>
      kclb.setEndpointConfiguration(new EndpointConfiguration(ep, endpoints.region))
    }

    kclb.build
  }

}
