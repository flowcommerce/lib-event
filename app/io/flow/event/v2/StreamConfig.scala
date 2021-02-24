package io.flow.event.v2

import com.amazonaws.auth.AWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClientBuilder}
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder, AmazonDynamoDBStreamsClientBuilder}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration}
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel
import io.flow.util.{FlowEnvironment, Naming}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.http.Protocol
import software.amazon.awssdk.http.nio.netty.{Http2Configuration, NettyNioAsyncHttpClient}
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient

import java.net.{InetAddress, URI}
import java.time.Duration
import java.util.UUID
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
  def endpoints: AWSEndpoints

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
}

case class KinesisStreamConfig(
  awsCredentialsProvider: AWSCreds,
  appName: String,
  streamName: String,
  maxRecords: Option[Int],   // number of records in each fetch
  idleMillisBetweenCalls: Option[Long],
  idleTimeBetweenReadsInMillis: Option[Long],
  maxLeasesForWorker: Option[Int],
  maxLeasesToStealAtOneTime: Option[Int],
  endpoints: AWSEndpoints,
) extends StreamConfig {

  val kinesisClient: KinesisAsyncClient = {
    // *** begin code copied from KinesisClientUtil ***
    val INITIAL_WINDOW_SIZE_BYTES = 512 * 1024 // 512 KB
    val HEALTH_CHECK_PING_PERIOD_MILLIS = 60 * 1000L

    val builder = KinesisAsyncClient.builder
      .httpClientBuilder(
        NettyNioAsyncHttpClient.builder
          .maxConcurrency(Integer.MAX_VALUE)
          .http2Configuration(
            Http2Configuration.builder
              .initialWindowSize(INITIAL_WINDOW_SIZE_BYTES)
              .healthCheckPingPeriod(Duration.ofMillis(HEALTH_CHECK_PING_PERIOD_MILLIS))
              .build
          )
          .protocol(Protocol.HTTP2)
    // *** end copied code ***
          .connectionTimeToLive(Duration.ofMillis(600000))
      )
      .credentialsProvider(awsCredentialsProvider.awsSDKv2Creds)
      .overrideConfiguration(
        ClientOverrideConfiguration.builder
          .retryPolicy(
            RetryPolicy.builder()
              .numRetries(10)
              .build
          )
          .build
      )

    for {
      k <- endpoints.kinesis
    } yield builder.endpointOverride(URI.create(k))

    builder.build
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
  override val endpoints: AWSEndpoints,
  eventClass: Type,
) extends StreamConfig {

  override def streamName: String = dynamoDBClient.describeTable(dynamoTableName).getTable.getLatestStreamArn

  def toKclConfig(creds: AWSCredentialsProviderChain): KinesisClientLibConfiguration = {
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

  val kinesisClient: AmazonDynamoDBStreamsAdapterClient = {
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

