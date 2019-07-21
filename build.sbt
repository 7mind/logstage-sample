import com.github.pshirshov.izumi.sbt.deps.Izumi.R
import com.github.pshirshov.izumi.sbt.deps.IzumiDeps

name := "logstage-sample"

version := "0.1"

scalaVersion := "2.12.8"

organization in ThisBuild := "com.ratoshniuk"

resolvers += Resolver.sonatypeRepo("releases")

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.9")

libraryDependencies ++= Seq(
  R.fundamentals_bio,
  IzumiDeps.R.zio_core,
  IzumiDeps.R.zio_interop,

  //logstage
  R.logstage_rendering_circe,
  R.logstage_core
)
