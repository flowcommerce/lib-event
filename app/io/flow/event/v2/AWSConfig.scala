package io.flow.event.v2

import com.amazonaws.SDKGlobalConfiguration
import com.amazonaws.auth._
import io.flow.play.util.Config
import javax.inject.Inject
import play.api.{Environment, Mode}

import scala.jdk.CollectionConverters._

class AWSCreds @Inject() (config: Config) extends AWSCredentialsProviderChain(

  List(

    for {
      accessKey <- config.optionalString("aws.access.key")
      secretKey <- config.optionalString("aws.secret.key")
    } yield new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)),

    // EC2 role
    Some(DefaultAWSCredentialsProviderChain.getInstance())

  ).flatten.asJava

)

/**
  * We run local versions of kinesis and dynamodb for testing through https://github.com/localstack/localstack
  */
class AWSEndpoints @Inject() (environment: Environment) {
  val region = "us-east-1"

  val kinesis = environment.mode match {
    case Mode.Test => Some("http://localhost:4568") // localstack
    case _ => None
  }

  val dynamodb = environment.mode match {
    case Mode.Test => Some("http://localhost:4569") // localstack
    case _ => None
  }

  val dynamodbStreams = environment.mode match {
    case Mode.Test => Some("http://localhost:4570") // localstack
    case _ => None
  }

  val cloudWatch = environment.mode match {
    case Mode.Test => Some("http://localhost:4582") // localstack
    case _ => None
  }

  environment.mode match {
    case Mode.Test =>
      // CBOR is a replacement for JSON. It is not yet supported by localstack
      System.setProperty(SDKGlobalConfiguration.AWS_CBOR_DISABLE_SYSTEM_PROPERTY, "true")
    case _ =>
  }
}
