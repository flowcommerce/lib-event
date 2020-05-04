package io.flow.event.v2

import java.util.concurrent.ConcurrentLinkedQueue

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import io.flow.event.Record
import io.flow.log.RollbarLogger
import io.flow.play.metrics.MetricsSystem
import io.flow.play.util.Config
import io.flow.util.FlowEnvironment
import javax.inject.{Inject, Singleton}

import scala.concurrent.duration._
import scala.reflect.runtime.universe._

trait DynamoStreamQueue extends Queue

class DefaultDynamoStreamQueue @Inject() (
  config: Config,
  creds: AWSCreds,
  endpoints: AWSEndpoints,
  metrics: MetricsSystem,
  logger: RollbarLogger
) extends Queue with StreamUsage with DynamoStreamQueue {

  import scala.jdk.CollectionConverters._

  private[this] val consumers = new ConcurrentLinkedQueue[DynamoStreamConsumer]()

  override def appName: String = config.requiredString("name")

  override def producer[T: TypeTag](numberShards: Int = 0): Producer[T] = sys.error("Not supported for Dynamo DB streams")

  override def consume[T: TypeTag](
    f: Seq[Record] => Unit,
    pollTime: FiniteDuration = 5.seconds
  ): Unit = {
    markConsumesStream(streamName[T], typeOf[T])
    consumers.add(
      DynamoStreamConsumer(
        streamConfig[T],
        creds,
        f,
        metrics,
        logger,
      )
    )
    ()
  }

  override def shutdownConsumers(): Unit = {
    // synchronized to avoid a consumer being registered "in between" shutdown and clear
    synchronized {
      consumers.asScala.foreach(_.shutdown())
      consumers.clear()
    }
  }

  override def shutdown(): Unit = shutdownConsumers()

  private[v2] def streamConfig[T: TypeTag] = {
    val tn = tableName[T]

    //fixme move
    val dynamoDBClientBuilder = AmazonDynamoDBClientBuilder.standard()
    endpoints.dynamodb.foreach {
      ep => dynamoDBClientBuilder.withEndpointConfiguration(new EndpointConfiguration(ep, endpoints.region))
    }
    val dynamoDBClient = dynamoDBClientBuilder.build()

    DynamoStreamConfig(
      appName = appName,
      dynamoTableName = tn,
      eventClass = typeOf[T],
      maxRecords = config.optionalInt(s"$tn.maxRecords"),
      idleMillisBetweenCalls = config.optionalLong(s"$tn.idleMillisBetweenCalls"),
      idleTimeBetweenReadsInMillis = config.optionalLong(s"$tn.idleTimeBetweenReadsMs"),
      maxLeasesForWorker = config.optionalInt(s"$tn.maxLeasesForWorker"),
      maxLeasesToStealAtOneTime = config.optionalInt(s"$tn.maxLeasesToStealAtOneTime"),
      endpoints = endpoints,
      dynamoDBClient = dynamoDBClient
    )
  }

  private[v2] def tableName[T: TypeTag] = s"${FlowEnvironment.Current}.${typeName[T]}s"
  private def typeName[T: TypeTag] = typeOf[T].typeSymbol.name.toString.toLowerCase
}

@Singleton
class MockDynamoStreamQueue @Inject()(logger: RollbarLogger) extends MockQueue(logger) with DynamoStreamQueue
