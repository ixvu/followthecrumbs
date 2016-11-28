name := "followthecrumbs"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0-preview" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0-preview" % "provided"
libraryDependencies  ++= Seq(
  // Last stable release
  "org.scalanlp" %% "breeze" % "0.12",

  // Native libraries are not included by default. add this if you want them (as of 0.7)
  // Native libraries greatly improve performance, but increase jar sizes.
  // It also packages various blas implementations, which have licenses that may or may not
  // be compatible with the Apache License. No GPL code, as best I know.
  "org.scalanlp" %% "breeze-natives" % "0.12",

  // The visualization library is distributed separately as well.
  // It depends on LGPL code
  "org.scalanlp" %% "breeze-viz" % "0.12"
)
libraryDependencies += "org.json4s" % "json4s-native_2.11" % "3.5.0"
libraryDependencies += "log4j" % "log4j" % "1.2.17"


resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
