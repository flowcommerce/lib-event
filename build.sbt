import sbt.Keys.scalacOptions

name := "lib-event-play26"

organization := "io.flow"

scalaVersion in ThisBuild := "2.12.8"

val libSuffix1 = ""
val libSuffix2 = "-play26"

lazy val root = project
  .in(file("."))
  .enablePlugins(PlayScala)
  .settings(
    testOptions += Tests.Argument("-oF"),
    libraryDependencies ++= Seq(
      ws,
      guice,
      "io.flow" %% s"lib-play$libSuffix2" % "0.5.42",
      "com.amazonaws" % "amazon-kinesis-client" % "1.9.3",
      // evict aws dependency on allegedly incompatible "jackson-dataformat-cbor" % "2.6.7",
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor" % "2.9.8",
      "org.mockito" % "mockito-core" % "2.23.4" % Test,
      "io.flow" %% s"lib-test-utils$libSuffix1" % "0.0.37" % Test,
      compilerPlugin("com.github.ghik" %% "silencer-plugin" % "1.3.0"),
      "com.github.ghik" %% "silencer-lib" % "1.3.0" % Provided
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
    javaOptions in Test += "-Dconfig.file=conf/test.conf",
    // silence all warnings on autogenerated files
    scalacOptions += "-P:silencer:pathFilters=test/generated/.*",
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

version := "0.4.55"
