package io.flow.event.v2

import com.amazonaws.auth._
import io.flow.play.util.Config
import javax.inject.Inject
import play.api.{Environment, Mode}

import scala.collection.JavaConverters._

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
    case Mode.Test => "http://localhost:4568" // localstack
    case _ => s"https://kinesis.$region.amazonaws.com"
  }

  val dynamodb = environment.mode match {
    case Mode.Test => "http://localhost:4569" // localstack
    case _ => s"https://dynamodb.$region.amazonaws.com"
  }
}
