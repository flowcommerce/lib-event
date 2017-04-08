package io.flow.event.v2

import com.amazonaws.auth.{AWSCredentials, AWSStaticCredentialsProvider}

case class StreamConfig(
  awsCredentials: AWSCredentials,
  appName: String,
  streamName: String
) {

  val aWSCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials)

}
