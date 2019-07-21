val izumi_version = "0.8.6"
// sbt toolkit
addSbtPlugin("io.7mind.izumi" % "sbt-izumi" % izumi_version)

// This is Izumi's BOM (Bill of Materials), see below
addSbtPlugin("io.7mind.izumi" % "sbt-izumi-deps" % izumi_version)