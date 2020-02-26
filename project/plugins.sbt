// Comment to get more information during initialization
logLevel := Level.Warn

credentials += Credentials(Path.userHome / ".ivy2" / ".artifactory")

// Use the Play sbt plugin for Play projects
addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.8.0")

addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat" % "0.1.10")

resolvers += "Flow Plugins" at "https://flow.jfrog.io/flow/plugins-release/"

addSbtPlugin("io.flow" % "sbt-flow-linter" % "0.0.16")
