package io.flow.event.v2

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider, BasicAWSCredentials}
import io.flow.play.util.Config

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
