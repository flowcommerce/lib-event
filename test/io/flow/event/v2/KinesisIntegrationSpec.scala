package io.flow.event.v2

import cloud.localstack.Localstack
import cloud.localstack.ServiceName.{KINESIS, DYNAMO}
import cloud.localstack.docker.LocalstackDocker
import cloud.localstack.docker.annotation.LocalstackDockerConfiguration
import org.scalatest.{BeforeAndAfterAll, Suite}
import sys.process._
import scala.language.postfixOps
import scala.collection.JavaConverters._

trait KinesisIntegrationSpec extends BeforeAndAfterAll { this: Suite =>
  val localstackDocker = LocalstackDocker.INSTANCE

  override def beforeAll(): Unit = {
    Localstack.teardownInfrastructure()

    val imagePresent = ("docker images" #| "grep localstack" !) == 0

    val conf = LocalstackDockerConfiguration.builder()
      .environmentVariables(Map("SERVICES" -> s"$KINESIS,$DYNAMO").asJava)
      .pullNewImage(!imagePresent)
      .build()

    println(conf)

    localstackDocker.startup(conf)
  }

  override def afterAll(): Unit = {
    localstackDocker.stop()
  }
}
