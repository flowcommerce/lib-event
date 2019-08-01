package io.flow.event.v2

import cloud.localstack.Localstack
import cloud.localstack.docker.LocalstackDocker
import cloud.localstack.docker.annotation.LocalstackDockerConfiguration
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.collection.JavaConverters._

trait KinesisIntegrationSpec extends BeforeAndAfterAll { this: Suite =>
  val localstackDocker = LocalstackDocker.INSTANCE

  override def beforeAll(): Unit = {
    Localstack.teardownInfrastructure()

    val conf = LocalstackDockerConfiguration.builder()
      .environmentVariables(Map("SERVICES" -> "kinesis,dynamodb").asJava)
      .build()

    println(conf)

    localstackDocker.startup(conf)
  }

  override def afterAll(): Unit = {
    localstackDocker.stop()
  }
}
