import play.sbt.PlayScala._

name := "lib-event-play26"

organization := "io.flow"

scalaVersion in ThisBuild := "2.12.6"

lazy val root = project
  .in(file("."))
  .enablePlugins(PlayScala)
  .settings(
    testOptions += Tests.Argument("-oF"),
    libraryDependencies ++= Seq(
      ws,
      guice,
      "io.flow" %% "lib-util" % "0.1.0",
      "io.flow" %% "lib-play-play26" % "0.5.5",
      "com.amazonaws" % "amazon-kinesis-client" % "1.9.2",
      // evict aws dependency on allegedly incompatible "jackson-dataformat-cbor" % "2.6.7",
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % "2.9.7",
      "org.mockito" % "mockito-core" % "2.22.0" % Test,
      "io.flow" %% "lib-test-utils" % "0.0.18" % Test
    ),
    resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
    resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases",
    resolvers += "Artifactory" at "https://flow.jfrog.io/flow/libs-release/",
    credentials += Credentials(
      "Artifactory Realm",
      "flow.jfrog.io",
      System.getenv("ARTIFACTORY_USERNAME"),
      System.getenv("ARTIFACTORY_PASSWORD")
    ),
    javaOptions in Test += "-Dconfig.file=conf/test.conf"
  )

publishTo := {
  val host = "https://flow.jfrog.io/flow"
  if (isSnapshot.value) {
    Some("Artifactory Realm" at s"$host/libs-snapshot-local;build.timestamp=" + new java.util.Date().getTime)
  } else {
    Some("Artifactory Realm" at s"$host/libs-release-local")
  }
}
version := "0.4.13"
