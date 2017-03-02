name := "zookeeper-helper"

version := "1.0"

scalaVersion := "2.12.1"


libraryDependencies += "org.apache.curator" % "curator-framework" % "2.10.0"

libraryDependencies += "commons-cli" % "commons-cli" % "1.3.1"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-log4j12" % "1.7.19",
  "log4j" % "log4j" % "1.2.17"
)
