import play.sbt.PlayScala._

name := "lib-event"

organization := "io.flow"

scalaVersion in ThisBuild := "2.12.3"

crossScalaVersions := Seq("2.12.3")

lazy val root = project
  .in(file("."))
  .enablePlugins(PlayScala)
  .settings(
    testOptions += Tests.Argument("-oF"),
    libraryDependencies ++= Seq(
      ws,
      "io.flow" %% "lib-play" % "0.4.2-SNAPSHOT",
      "com.amazonaws" % "amazon-kinesis-client" % "1.8.1",
      "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.1" % "test",
      "org.mockito" % "mockito-core" % "2.9.0" % "test"
    ),
    resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
    resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases",
    resolvers += "Artifactory" at "https://flow.artifactoryonline.com/flow/libs-release/",
    resolvers += "Artifactory-Snapshots-Local" at "https://flow.artifactoryonline.com/flow/libs-snapshot-local/",
    resolvers += "Artifactory-Releases-Local" at "https://flow.artifactoryonline.com/flow/libs-release-local/",
    credentials += Credentials(
      "Artifactory Realm",
      "flow.artifactoryonline.com",
      System.getenv("ARTIFACTORY_USERNAME"),
      System.getenv("ARTIFACTORY_PASSWORD")
    ),
    javaOptions in Test += "-Dconfig.file=conf/test.conf"
)

publishTo := {
  val host = "https://flow.artifactoryonline.com/flow"
  if (isSnapshot.value) {
    Some("Artifactory Realm" at s"$host/libs-snapshot-local;build.timestamp=" + new java.util.Date().getTime)
  } else {
    Some("Artifactory Realm" at s"$host/libs-release-local")
  }
}
version := "0.3.0-SNAPSHOT"
