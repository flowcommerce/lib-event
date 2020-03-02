import sbt.Keys.scalacOptions

name := "lib-event-play28"

organization := "io.flow"

scalaVersion := "2.13.1"

lazy val root = project
  .in(file("."))
  .enablePlugins(PlayScala)
  .settings(
    javaOptions in Test += "-Dkamon.show-aspectj-missing-warning=no",
    testOptions += Tests.Argument("-oF"),
    libraryDependencies ++= Seq(
      ws,
      guice,
      "io.flow" %% "lib-akka-akka26" % "0.1.14",
      "io.flow" %% "lib-play-graphite-play28" % "0.1.36",
      "com.amazonaws" % "amazon-kinesis-client" % "1.13.2",
      // evict aws dependency on allegedly incompatible "jackson-dataformat-cbor" % "2.6.7",
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % "2.10.2",
      "org.mockito" % "mockito-core" % "3.2.4" % Test,
      "io.flow" %% "lib-test-utils-play28" % "0.0.79" % Test,
      compilerPlugin("com.github.ghik" %% "silencer-plugin" % "1.4.4" cross CrossVersion.full),
      "com.github.ghik" %% "silencer-lib" % "1.4.4" % Provided cross CrossVersion.full,
      "cloud.localstack" % "localstack-utils" % "0.1.22" % Test,
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
    javaOptions in Test += "-Dconfig.file=conf/test.conf",
    // silence all warnings on autogenerated files
    flowGeneratedFiles ++= Seq(
      "test/generated/.*".r,
    ),
    // Make sure you only exclude warnings for the project directories, i.e. make builds reproducible
    scalacOptions += s"-P:silencer:sourceRoots=${baseDirectory.value.getCanonicalPath}",
  )

publishTo := {
  val host = "https://flow.jfrog.io/flow"
  if (isSnapshot.value) {
    Some("Artifactory Realm" at s"$host/libs-snapshot-local;build.timestamp=" + new java.util.Date().getTime)
  } else {
    Some("Artifactory Realm" at s"$host/libs-release-local")
  }
}

version := "1.0.31"
