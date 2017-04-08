package io.flow.event.v2

import java.net.InetAddress
import java.util.UUID
import javax.inject.Inject

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicAWSCredentials}
import io.flow.event.{Record, StreamNames}
import io.flow.play.util.Config
import play.api.libs.json.JsValue
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.{IRecordProcessor, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.clientlibrary.types.{InitializationInput, ProcessRecordsInput, ShutdownInput}
import org.joda.time.DateTime

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._
import collection.JavaConverters._

trait Queue {

  def consume[T: TypeTag](
    function: Record => Unit
  )(
    implicit ec: ExecutionContext
  )

}

//TODO:
trait Publisher {
  def publish(event: JsValue)
}

class KinesisConsumer @Inject() (
  config: Config
) extends Queue {

  override def consume[T: TypeTag](
    function: Record => Unit
  )(
    implicit ec: ExecutionContext
  ) {
    val streamName = StreamNames.fromType[T] match {
      case Left(errors) => sys.error(errors.mkString(", "))
      case Right(name) => name
    }

    val flowStreamConfig = FlowStreamConfig(
      appName = config.requiredString("name"),
      streamName = streamName,
      awsCredentialsProvider = FlowConfigAWSCredentialsProvider(config),
      function = function
    )

    val workerId = InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID

    val kinesisConfig = new KinesisClientLibConfiguration(
      flowStreamConfig.appName,
      flowStreamConfig.streamName,
      flowStreamConfig.awsCredentialsProvider,
      workerId
    ).withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)

    new Worker.Builder()
      .recordProcessorFactory(KinesisRecordProcessorFactory(flowStreamConfig))
      .config(kinesisConfig)
      .build()
      .run()
  }
}

case class FlowStreamConfig(
  awsCredentialsProvider: AWSCredentialsProvider,
  appName: String,
  streamName: String,
  function: Record => Unit
)

case class FlowConfigAWSCredentialsProvider(config: Config) extends AWSCredentialsProvider {

  override def getCredentials: AWSCredentials = {
    new BasicAWSCredentials(
      config.requiredString("aws.access.key"),
      config.requiredString("aws.secret.key")
    )
  }

  override def refresh(): Unit = {
    // no-op
  }

}

case class KinesisRecordProcessorFactory(config: FlowStreamConfig) extends IRecordProcessorFactory {

  override def createProcessor(): IRecordProcessor = {
    KinesisRecordProcessor(config)
  }

}

case class KinesisRecordProcessor[T](
  config: FlowStreamConfig
) extends IRecordProcessor {

  override def initialize(input: InitializationInput): Unit = {
    println(s"initializing stream[${config.streamName}] shard[${input.getShardId}]")
  }

  override def processRecords(input: ProcessRecordsInput): Unit = {
    println("processRecords  stream[${config.streamName}] starting")
    input.getRecords.asScala.foreach { record =>
      val buffer = record.getData
      val bytes = Array.fill[Byte](buffer.remaining)(0)
      buffer.get(bytes)

      val flowRecord = Record.fromByteArray(
        arrivalTimestamp = new DateTime(record.getApproximateArrivalTimestamp),
        value = bytes
      )

      println("processRecords  stream[${config.streamName}] flowRecord: $flowRecord")
      config.function(flowRecord)
    }
  }

  override def shutdown(input: ShutdownInput): Unit = {
    println(s"shutting down stream[${config.streamName}] reason[${input.getShutdownReason}]")
  }

}

/*
  private[this] val numberShards = 1
  private[this] val kinesisClient = AmazonKinesisClientBuilder.standard().
    withCredentials(new AWSStaticCredentialsProvider(credentials)).
    withClientConfiguration(clientConfig).
    build()


  println("Using workerId: " + workerId)
}
*/