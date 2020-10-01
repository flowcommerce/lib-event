package io.flow.event.v2

import java.util.concurrent.ConcurrentLinkedQueue

import io.flow.event.Record
import io.flow.log.RollbarLogger
import io.flow.play.metrics.MetricsSystem
import io.flow.play.util.Config
import io.flow.util.{FlowEnvironment, StreamNames}
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
    DynamoStreamConfig(
      appName = appName,
      dynamoTableName = tn,
      eventClass = typeOf[T],
      maxRecords = config.optionalInt(s"$tn.maxRecords"),
      idleMillisBetweenCalls = config.optionalLong(s"$tn.idleMillisBetweenCalls"),
      idleTimeBetweenReadsInMillis = config.optionalLong(s"$tn.idleTimeBetweenReadsMs"),
      maxLeasesForWorker = config.optionalInt(s"$tn.maxLeasesForWorker"),
      maxLeasesToStealAtOneTime = config.optionalInt(s"$tn.maxLeasesToStealAtOneTime"),
      endpoints = endpoints
    )
  }

  private[v2] def tableName[T: TypeTag] = s"${FlowEnvironment.Current}.${tableNameFromType[T]}s"
  private def tableNameFromType[T: TypeTag]: String = {
    val typ = typeOf[T].typeSymbol.name.toString
    StreamNames.toSnakeCase(typ)
  }
}

@Singleton
class MockDynamoStreamQueue @Inject()(logger: RollbarLogger) extends MockQueue(logger) with DynamoStreamQueue
