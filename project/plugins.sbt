// Comment to get more information during initialization
logLevel := Level.Warn

credentials += Credentials(Path.userHome / ".ivy2" / ".artifactory")

// Use the Play sbt plugin for Play projects
addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.6.7")
