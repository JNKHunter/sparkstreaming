name := "sparkstreaming"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.1.1"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
