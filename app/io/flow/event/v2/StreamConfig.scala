case class StreamConfig(
  awsCredentials: AWSCredentials,
  appName: String,
  streamName: String
) {

  val aWSCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials)

}
