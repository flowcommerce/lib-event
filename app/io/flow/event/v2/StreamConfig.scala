package io.flow.event.v2

import java.net.InetAddress
import java.time.Duration
import java.util.UUID

import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import io.flow.util.{FlowEnvironment, Naming}
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain
import software.amazon.awssdk.core.client.config.{ClientAsyncConfiguration, ClientOverrideConfiguration}
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.kinesis.common.{ConfigsBuilder, InitialPositionInStream, InitialPositionInStreamExtended, KinesisClientUtil}
import software.amazon.kinesis.metrics.MetricsLevel
import software.amazon.kinesis.processor.ShardRecordProcessorFactory
import software.amazon.kinesis.retrieval.polling.{PollingConfig, SimpleRecordsFetcherFactory}

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

  def kinesisClient: KinesisAsyncClient

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

case class DefaultStreamConfig(
  awsCredentialsProvider: AWSCreds,
  appName: String,
  streamName: String,
  maxRecords: Option[Int],   // number of records in each fetch
  idleMillisBetweenCalls: Option[Long],
  idleTimeBetweenReadsInMillis: Option[Long],
  maxLeasesForWorker: Option[Int],
  maxLeasesToStealAtOneTime: Option[Int],
  eventClass: Type
) extends StreamConfig {

  override def kinesisClient: KinesisAsyncClient = {
    val httpClientBuilder = NettyNioAsyncHttpClient.builder()
      .connectionTimeToLive(Duration.ofMillis(600000))

    val asyncConfig = ClientAsyncConfiguration.builder()
        .build()

    val clientBuilder = KinesisAsyncClient.builder()
      .credentialsProvider(awsCredentialsProvider.creds)
      .httpClientBuilder(httpClientBuilder)
      .asyncConfiguration(asyncConfig)


    val retryPolicy = RetryPolicy.builder()
      .numRetries(10)
      .build()

    val overrideConfig = ClientOverrideConfiguration.builder()
      .retryPolicy(retryPolicy)
      .build()

    KinesisClientUtil.adjustKinesisClientBuilder(clientBuilder)
      .overrideConfiguration(overrideConfig)
      .build()
  }
}

case class ConsumerConfig(config: StreamConfig, creds: AwsCredentialsProviderChain, recordProcessorFactory: ShardRecordProcessorFactory) {
  private val configsBuilder = {
    val dynamoClient = DynamoDbAsyncClient.builder.credentialsProvider(creds).build
    val cloudWatchClient = CloudWatchAsyncClient.builder.credentialsProvider(creds).build
    new ConfigsBuilder(config.streamName, config.appName, config.kinesisClient, dynamoClient, cloudWatchClient, config.workerId, recordProcessorFactory)
  }

  private val dynamoCapacity = {
    FlowEnvironment.Current match {
      case FlowEnvironment.Production => 10 // 10 is the default value in the AWS SDK
      case FlowEnvironment.Development | FlowEnvironment.Workstation => 1
    }
  }

  val checkpointConfig = configsBuilder.checkpointConfig()

  val coordinatorConfig = configsBuilder.coordinatorConfig()
    .shardConsumerDispatchPollIntervalMillis(config.idleTimeBetweenReadsInMillis.getOrElse(configsBuilder.coordinatorConfig().shardConsumerDispatchPollIntervalMillis))

  val leaseManagementConfig = configsBuilder.leaseManagementConfig()
    .initialLeaseTableReadCapacity(dynamoCapacity)
    .initialLeaseTableWriteCapacity(dynamoCapacity)
    .initialPositionInStream(InitialPositionInStreamExtended.newInitialPosition(InitialPositionInStream.TRIM_HORIZON))
    .cleanupLeasesUponShardCompletion(true)
    .maxLeasesForWorker(config.maxLeasesForWorker.getOrElse(configsBuilder.leaseManagementConfig.maxLeasesForWorker))
    .maxLeasesToStealAtOneTime(config.maxLeasesToStealAtOneTime.getOrElse(configsBuilder.leaseManagementConfig.maxLeasesToStealAtOneTime))
    .failoverTimeMillis(30000)

  val lifecycleConfig = configsBuilder.lifecycleConfig()

  val metricsConfig = configsBuilder.metricsConfig().metricsLevel(MetricsLevel.NONE)

  val processorConfig = configsBuilder.processorConfig()

  val recordsFetcherFactory = {
    val f = new SimpleRecordsFetcherFactory()
    config.idleMillisBetweenCalls.foreach(f.idleMillisBetweenCalls)
    f
  }

  val pollingConfig = new PollingConfig(config.streamName, config.kinesisClient)
    .maxRecords(config.maxRecords.getOrElse(1000))
    .idleTimeBetweenReadsInMillis(config.idleTimeBetweenReadsInMillis.getOrElse(configsBuilder.coordinatorConfig().shardConsumerDispatchPollIntervalMillis))
    .recordsFetcherFactory(recordsFetcherFactory)

  val retrievalConfig = configsBuilder.retrievalConfig()
    .retrievalSpecificConfig(pollingConfig)
}