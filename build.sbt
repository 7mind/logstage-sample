name := "scala-ua-2019"

version := "0.1"

scalaVersion := "2.12.8"

val izumi_version = "0.7.0-SNAPSHOT"
// LogStage API, you need it to use the logger
libraryDependencies += "com.github.pshirshov.izumi.r2" %% "logstage-core" % izumi_version

// LogStage machinery
libraryDependencies ++= Seq(
  // json output
  "com.github.pshirshov.izumi.r2" %% "logstage-rendering-circe" % izumi_version
  // router from Slf4j to LogStage
  , "com.github.pshirshov.izumi.r2" %% "logstage-adapter-slf4j" % izumi_version
  , "com.github.pshirshov.izumi.r2" %% "logstage-zio" % izumi_version
)
