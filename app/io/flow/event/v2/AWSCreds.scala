package io.flow.event.v2

import com.amazonaws.auth._
import io.flow.play.util.Config
import javax.inject.Inject

import scala.collection.JavaConverters._

class AWSCreds @Inject() (config: Config) extends AWSCredentialsProviderChain(

  List(

    // for EC2 role
    Some(DefaultAWSCredentialsProviderChain.getInstance()),

    // fallback to known working config
    for {
      accessKey <- config.optionalString("aws.access.key")
      secretKey <- config.optionalString("aws.secret.key")
    } yield new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey))

  ).flatten.asJava

)
