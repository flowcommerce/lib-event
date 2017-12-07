import play.sbt.PlayScala._

name := "lib-event-play-26"

organization := "io.flow"

scalaVersion in ThisBuild := "2.12.4"

crossScalaVersions := Seq("2.11.12", "2.12.4")

lazy val root = project
  .in(file("."))
  .enablePlugins(PlayScala)
  .settings(
    testOptions += Tests.Argument("-oF"),
    libraryDependencies ++= Seq(
      ws,
      "io.flow" %% "lib-play-play-26" % "0.4.7",
     "com.amazonaws" % "amazon-kinesis-client" % "1.8.8",
      "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.1" % Test,
      "org.mockito" % "mockito-core" % "2.13.0" % "test"
    ),
    resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
    resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases",
    resolvers += "Artifactory" at "https://flow.artifactoryonline.com/flow/libs-release/",
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
version := "0.2.78-play26"
