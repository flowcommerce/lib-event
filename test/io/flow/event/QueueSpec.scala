package io.flow.event

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient
import com.amazonaws.services.cloudwatch.model._
import io.flow.play.util.{FlowEnvironment, Random}
import org.joda.time.DateTime
import org.scalatestplus.play.{OneAppPerSuite, PlaySpec}

class QueueSpec extends PlaySpec with OneAppPerSuite {

  private[this] val dev = StreamNames(FlowEnvironment.Development)
  private[this] val ws = StreamNames(FlowEnvironment.Workstation)
  private[this] val prod = StreamNames(FlowEnvironment.Production)

  private[this] val credentials = new BasicAWSCredentials(
    "AKIAICQQ3B2RB6XNM5OA",
    "+sU72WOZV4/LUU1/57KCZqNqMwnVWsWOdJSj/Ks7"
  )
  "put metric data" in {
    val cloudWatchClient = new AmazonCloudWatchClient(credentials)

    val now = DateTime.now()

    (1 to 1000).foreach { _ =>
      val latency = now.minus(now.minus(scala.util.Random.nextInt(1000)).getMillis).getMillis.toDouble
      println(s"Mock Stream Latency: $latency ms")
      cloudWatchClient.putMetricData(
        new PutMetricDataRequest()
          .withNamespace("Roth") //namespace from documentation: http://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/ak-metricscollected.html#kinesis-metrics-stream
          .withMetricData(
          new MetricDatum()
            .withMetricName("StreamLatency")
            .withDimensions(
              new Dimension()
                .withName("StreamName")
                .withValue("roth-test-stream"),
              new Dimension()
                .withName("ShardId")
                .withValue("roth-test-000")
            )
            .withValue(now.minus(now.minus(scala.util.Random.nextInt(1000)).getMillis).getMillis.toDouble)
            .withUnit(StandardUnit.Milliseconds)
        )
      )
    }

  }

}
