package io.flow.event.v2

import java.net.InetAddress
import java.util.UUID

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.AWSCredentialsProvider
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
  def awsCredentialsProvider: AWSCredentialsProvider
  def eventClass: Type
  def workerId: String

  def kinesisClient: AmazonKinesis

  def dynamoTableName: String = {
    Naming.dynamoKinesisTableName(
      streamName = streamName,
      appName = appName
    )
  }

  def toKclConfig: KinesisClientLibConfiguration
}

case class DefaultStreamConfig(
  awsCredentialsProvider: AWSCreds,
  appName: String,
  streamName: String,
  maxRecords: Option[Int],   // number of records in each fetch
  idleMillisBetweenCalls: Option[Long],
  idleTimeBetweenReadsInMillis: Option[Long],
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

  override lazy val toKclConfig = {
    new KinesisClientLibConfiguration(
      appName,
      streamName,
      awsCredentialsProvider,
      workerId
    ).withTableName(dynamoTableName)
      .withInitialLeaseTableReadCapacity(dynamoCapacity)
      .withInitialLeaseTableWriteCapacity(dynamoCapacity)
      .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
      .withCleanupLeasesUponShardCompletion(true)
      .withIdleMillisBetweenCalls(idleMillisBetweenCalls.getOrElse(1500L))
      .withIdleTimeBetweenReadsInMillis(idleTimeBetweenReadsInMillis.getOrElse(KinesisClientLibConfiguration.DEFAULT_IDLETIME_BETWEEN_READS_MILLIS))
      .withMaxRecords(maxRecords.getOrElse(1000))
      .withMetricsLevel(MetricsLevel.NONE)
      .withFailoverTimeMillis(30000) // See https://github.com/awslabs/amazon-kinesis-connectors/issues/10
  }

  override lazy val workerId = Seq(
    appName,
    InetAddress.getLocalHost.getCanonicalHostName,
    UUID.randomUUID.toString
  ).mkString(":")

  private[this] val dynamoCapacity = {
    FlowEnvironment.Current match {
      case FlowEnvironment.Production => 10 // 10 is the default value in the AWS SDK
      case FlowEnvironment.Development | FlowEnvironment.Workstation => 1
    }
  }
}
