package io.flow.event.v2

import java.net.URI

import io.flow.play.util.Config
import javax.inject.Inject
import play.api.{Environment, Mode}
import software.amazon.awssdk.auth.credentials._

class AWSCreds @Inject() (config: Config) {


  val creds = AwsCredentialsProviderChain.of(
    List(
      for {
        accessKey <- config.optionalString("aws.access.key")
        secretKey <- config.optionalString("aws.secret.key")
      } yield StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)),

      // EC2 role
      Some(DefaultCredentialsProvider.create())

    ).flatten.toArray: _*
  )
}

/**
  * We run local versions of kinesis and dynamodb for testing through https://github.com/localstack/localstack
  */
class AWSEndpoints @Inject() (environment: Environment) {
  val region = "us-east-1"

  val kinesis = environment.mode match {
    case Mode.Test => URI.create("http://localhost:4568") // localstack
    case _ => URI.create(s"https://kinesis.$region.amazonaws.com")
  }

  val dynamodb = environment.mode match {
    case Mode.Test => URI.create("http://localhost:4569") // localstack
    case _ => URI.create(s"https://dynamodb.$region.amazonaws.com")
  }
}
