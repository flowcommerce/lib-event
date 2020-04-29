package io.flow.event.v2

import java.util.concurrent.ConcurrentLinkedQueue

import io.flow.event.Record
import io.flow.log.RollbarLogger
import io.flow.play.metrics.MetricsSystem
import io.flow.play.util.Config
import io.flow.util.StreamNames
import javax.inject.Inject

import scala.concurrent.duration.FiniteDuration
import scala.reflect.runtime.universe._

trait Queue {

  def producer[T: TypeTag](
    numberShards: Int = 1
  ): Producer[T]

  /**
    * Creates a thread that will poll kinesis on the specified interval,
    * invoking your provided function for each new record
    */
  def consume[T: TypeTag](
    f: Seq[Record] => Unit,
    pollTime: FiniteDuration = FiniteDuration(5, "seconds")
  ): Unit

  def shutdown(): Unit

  def shutdownConsumers(): Unit

  /** Return the name of this stream [if it were on AWS] */
  def streamName[T: TypeTag]: String = {
    StreamNames.fromType[T] match {
      case Left(errors) => sys.error(errors.mkString(", "))
      case Right(name) => name
    }
  }
  /**
    * The name of the application which is used by the kinesis client library
    * to manage leases, ensuring only one consumer is running for all nodes
    * with the same name. This defaults to the 'name' configuration parameter.
    */
  def appName: String
}

trait Producer[T] {

  def publish[U <: T](
    event: U,
    shardProvider: KinesisShardProvider[U] = OrganizationOrEventIdShardProvider[U]
  )(implicit serializer: play.api.libs.json.Writes[U]): Unit

  def publishBatch[U <: T](
    events: Seq[U],
    shardProvider: KinesisShardProvider[U] = OrganizationOrEventIdShardProvider[U]
  )(implicit serializer: play.api.libs.json.Writes[U]): Unit =
    events.foreach(publish(_, shardProvider))

  def shutdown(): Unit

}


/**
  * Builds our default producer/consumer
  */
class DefaultQueue @Inject() (
  config: Config,
  creds: AWSCreds,
  endpoints: AWSEndpoints,
  metrics: MetricsSystem,
  logger: RollbarLogger,
) extends Queue with StreamUsage {

  import scala.jdk.CollectionConverters._

  private[this] val consumers = new ConcurrentLinkedQueue[KinesisConsumer]()

  override def appName: String = config.requiredString("name")

  override def producer[T: TypeTag](
    numberShards: Int = 1,
  ): Producer[T] = {
    markProducesStream(streamName[T], typeOf[T])
    KinesisProducer(
      streamConfig[T],
      numberShards,
      logger
    )
  }

  override def consume[T: TypeTag](
     f: Seq[Record] => Unit,
     pollTime: FiniteDuration = FiniteDuration(5, "seconds")
  ): Unit = {
    markConsumesStream(streamName[T], typeOf[T])
    consumers.add(
      DefaultKinesisConsumer(
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

  protected[v2] def streamConfig[T: TypeTag] = {
    val sn = streamName[T]
    DefaultStreamConfig(
      awsCredentialsProvider = creds,
      appName = appName,
      streamName = sn,
      eventClass = typeOf[T],
      maxRecords = config.optionalInt(s"$sn.maxRecords"),
      idleMillisBetweenCalls = config.optionalLong(s"$sn.idleMillisBetweenCalls"),
      idleTimeBetweenReadsInMillis = config.optionalLong(s"$sn.idleTimeBetweenReadsMs"),
      maxLeasesForWorker = config.optionalInt(s"$sn.maxLeasesForWorker"),
      maxLeasesToStealAtOneTime = config.optionalInt(s"$sn.maxLeasesToStealAtOneTime"),
      endpoints = endpoints,
    )
  }
}