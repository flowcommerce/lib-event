package io.flow.event.v2

import java.net.InetAddress
import java.util.UUID

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClientBuilder}
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder, AmazonDynamoDBStreamsClientBuilder}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration}
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}
import io.flow.util.{FlowEnvironment, Naming}

import scala.annotation.nowarn
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

    @nowarn("cat=deprecation") // https://github.com/awslabs/amazon-kinesis-client/issues/737
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

case class DynamoStreamConfig(
  override val appName: String,
  override val dynamoTableName: String,
  override val maxRecords: Option[Int],
  override val idleMillisBetweenCalls: Option[Long],
  override val idleTimeBetweenReadsInMillis: Option[Long],
  override val maxLeasesForWorker: Option[Int],
  override val maxLeasesToStealAtOneTime: Option[Int],
  override val eventClass: Type,
  override val endpoints: AWSEndpoints
) extends StreamConfig {

  override def streamName: String = dynamoDBClient.describeTable(dynamoTableName).getTable.getLatestStreamArn

  override def toKclConfig(creds: AWSCredentialsProviderChain): KinesisClientLibConfiguration = {
    val dynamoCapacity = {
      FlowEnvironment.Current match {
        case FlowEnvironment.Production => 10 // 10 is the default value in the AWS SDK
        case FlowEnvironment.Development | FlowEnvironment.Workstation => 1
      }
    }

    @nowarn("cat=deprecation")
    val kclConf = new KinesisClientLibConfiguration(appName, streamName, creds, workerId)
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

    endpoints.kinesis.foreach(kclConf.withKinesisEndpoint)
    endpoints.dynamodb.foreach(kclConf.withDynamoDBEndpoint)
    kclConf
  }

  override def kinesisClient: AmazonDynamoDBStreamsAdapterClient = {
    val builder = AmazonDynamoDBStreamsClientBuilder.standard()
    endpoints.dynamodbStreams.foreach { ep =>
      builder.withEndpointConfiguration(new EndpointConfiguration(ep, endpoints.region))
    }
    new AmazonDynamoDBStreamsAdapterClient(builder.build)
  }

  val dynamoDBClient: AmazonDynamoDB = {
    val builder = AmazonDynamoDBClientBuilder.standard()
    endpoints.dynamodb.foreach {
      ep => builder.withEndpointConfiguration(new EndpointConfiguration(ep, endpoints.region))
    }
    builder.build()
  }

  val cloudWatchClient: AmazonCloudWatch = {
    val builder = AmazonCloudWatchClientBuilder.standard()
    endpoints.cloudWatch.foreach { ep =>
      builder.withEndpointConfiguration(new EndpointConfiguration(ep, endpoints.region))
    }
    builder.build()
  }
}

