// Comment to get more information during initialization
logLevel := Level.Warn

credentials += Credentials(Path.userHome / ".ivy2" / ".artifactory")

// Use the Play sbt plugin for Play projects
addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.6.20")

addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat" % "0.1.4")

addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.9.2")
