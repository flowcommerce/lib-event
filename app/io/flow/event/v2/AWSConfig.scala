package io.flow.event.v2

import java.net.URI

import io.flow.play.util.Config
import javax.inject.Inject
import play.api.{Environment, Mode}
import software.amazon.awssdk.auth.credentials._
import software.amazon.awssdk.core.SdkSystemSetting
import software.amazon.awssdk.http.Protocol

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
    case Mode.Test => Some(URI.create("http://localhost:4568"))
    case _ => None
  }

  val dynamodb = environment.mode match {
    case Mode.Test => Some(URI.create("http://localhost:4569"))
    case _ => None
  }

  val protocol = environment.mode match {
    case Mode.Test => Some(Protocol.HTTP1_1)
    case _ => None
  }

  environment.mode match {
    case Mode.Test =>
      // CBOR is a replacement for JSON. It is not yet supported by localstack
      System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false")
    case _ =>
  }
}
