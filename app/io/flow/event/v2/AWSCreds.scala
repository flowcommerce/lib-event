package io.flow.event.v2

import software.amazon.awssdk.auth.credentials._
import io.flow.play.util.Config
import javax.inject.Inject

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
