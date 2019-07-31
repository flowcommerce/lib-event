package io.flow.event.v2

import cloud.localstack.Localstack
import cloud.localstack.docker.LocalstackDocker
import cloud.localstack.docker.annotation.LocalstackDockerConfiguration
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.collection.JavaConverters._

trait KinesisIntegrationSpec extends BeforeAndAfterAll { this: Suite =>
  val localstack = LocalstackDocker.INSTANCE

  override def beforeAll(): Unit = {
    Localstack.teardownInfrastructure()

    localstack.startup(
      LocalstackDockerConfiguration.builder()
        .environmentVariables(Map("SERVICES" -> "kinesis,dynamodb").asJava)
        .build()
    )
  }

  override def afterAll(): Unit = {
    localstack.stop()
  }
}
