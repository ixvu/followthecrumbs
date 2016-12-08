name := "followthecrumbs"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0-preview" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0-preview" % "provided"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.0.0-preview" % "provided"
libraryDependencies += "org.json4s" % "json4s-native_2.11" % "3.5.0"
libraryDependencies += "log4j" % "log4j" % "1.2.17" % "provided"


resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
