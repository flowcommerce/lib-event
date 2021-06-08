import sbt.Keys.scalacOptions

name := "lib-event-play28"

organization := "io.flow"

scalaVersion := "2.13.5"

lazy val allScalacOptions = Seq(
  "-deprecation",
  "-feature",
  "-Xfatal-warnings",
  "-unchecked",
  "-Xcheckinit",
  "-Xlint:adapted-args",
  "-Ypatmat-exhaust-depth", "100", // Fixes: Exhaustivity analysis reached max recursion depth, not all missing cases are reported.
  "-Wconf:src=generated/.*:silent",
  "-Wconf:src=target/.*:silent", // silence the unused imports errors generated by the Play Routes
)

lazy val root = project
  .in(file("."))
  .enablePlugins(PlayScala)
  .settings(
    Test / javaOptions += "-Dkamon.show-aspectj-missing-warning=no",
    testOptions += Tests.Argument("-oF"),
    libraryDependencies ++= Seq(
      ws,
      guice,
      "io.flow" %% "lib-akka-akka26" % "0.1.51",
      "io.flow" %% "lib-play-graphite-play28" % "0.1.96",
      "com.amazonaws" % "amazon-kinesis-client" % "1.14.3",
      "com.amazonaws" % "dynamodb-streams-kinesis-adapter" % "1.5.3",
      "software.amazon.kinesis" % "amazon-kinesis-client" % "2.3.4",
      // evict aws dependency on allegedly incompatible "jackson-dataformat-cbor" % "2.6.7",
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % "2.10.3",
      "org.mockito" % "mockito-core" % "3.11.0" % Test,
      "io.flow" %% "lib-test-utils-play28" % "0.1.33" % Test,
      "org.scalatestplus" %% "mockito-3-2" % "3.1.2.0" % Test,
      "cloud.localstack" % "localstack-utils" % "0.2.11" % Test
    ),
    resolvers += "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
    resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases",
    resolvers += "Artifactory" at "https://flow.jfrog.io/flow/libs-release/",
    credentials += Credentials(
      "Artifactory Realm",
      "flow.jfrog.io",
      System.getenv("ARTIFACTORY_USERNAME"),
      System.getenv("ARTIFACTORY_PASSWORD")
    ),
    Test / javaOptions += "-Dconfig.file=conf/test.conf",
    scalacOptions ++= allScalacOptions,
  )

publishTo := {
  val host = "https://flow.jfrog.io/flow"
  if (isSnapshot.value) {
    Some("Artifactory Realm" at s"$host/libs-snapshot-local;build.timestamp=" + new java.util.Date().getTime)
  } else {
    Some("Artifactory Realm" at s"$host/libs-release-local")
  }
}

