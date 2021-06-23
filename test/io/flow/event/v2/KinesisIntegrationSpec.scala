package io.flow.event.v2

import cloud.localstack.Localstack
import cloud.localstack.ServiceName.{CLOUDWATCH, DYNAMO, DYNAMO_STREAMS, KINESIS}
import cloud.localstack.docker.annotation.LocalstackDockerConfiguration
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.jdk.CollectionConverters._
import scala.language.postfixOps
import scala.sys.process._

trait KinesisIntegrationSpec extends BeforeAndAfterAll { this: Suite =>
  private val localstack = Localstack.INSTANCE

  override def beforeAll(): Unit = {
    val imagePresent = ("docker images" #| "grep localstack" !) == 0

    val conf = LocalstackDockerConfiguration.builder()
      .environmentVariables(Map("SERVICES" -> s"$KINESIS,$DYNAMO,$DYNAMO_STREAMS,$CLOUDWATCH").asJava)
      .imageName("localstack/localstack")
      .imageTag("0.12.12")
      .pullNewImage(!imagePresent)
      .build()

    println(conf)

    localstack.startup(conf)
  }

  override def afterAll(): Unit = {
    localstack.stop()
  }
}
