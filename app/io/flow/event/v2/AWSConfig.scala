package io.flow.event.v2

import com.amazonaws.auth._
import io.flow.play.util.Config
import play.api.{Environment, Mode}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, AwsCredentialsProviderChain, DefaultCredentialsProvider, StaticCredentialsProvider}
import software.amazon.awssdk.core.SdkSystemSetting

import javax.inject.Inject
import scala.jdk.CollectionConverters._

class AWSCreds @Inject() (config: Config) {

  private val keys = for {
    accessKey <- config.optionalString("aws.access.key")
    secretKey <- config.optionalString("aws.secret.key")
  } yield (accessKey, secretKey)

  lazy val awsSDKv1Creds =
    new AWSCredentialsProviderChain(
      List(
        keys.map { case (accessKey, secretKey) =>
          new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey))
        },
        Some(DefaultAWSCredentialsProviderChain.getInstance())
      ).flatten.asJava
    )

  lazy val awsSDKv2Creds =
    AwsCredentialsProviderChain.of(
      List(
        keys.map { case (accessKey, secretKey) =>
          StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))
        },
        Some(DefaultCredentialsProvider.create())
      ).flatten: _*
    )

}

/**
  * We run local versions of kinesis and dynamodb for testing through https://github.com/localstack/localstack
  */
class AWSEndpoints @Inject() (environment: Environment) {
  val region = "us-east-1"

  private val localstackInTest = environment.mode match {
    case Mode.Test => Some("http://localhost:4566") // localstack
    case _ => None
  }

  val kinesis = localstackInTest
  val dynamodb = localstackInTest
  val dynamodbStreams = localstackInTest
  val cloudWatch = localstackInTest

  environment.mode match {
    case Mode.Test =>
      // CBOR is a replacement for JSON. It is not yet supported by localstack
      System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false")
    case _ =>
  }
}
