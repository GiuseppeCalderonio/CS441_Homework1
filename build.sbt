name := "Homework1"

version := "0.1"

scalaVersion := "3.0.2"

val logbackVersion = "1.3.0-alpha10"
val sfl4sVersion = "2.0.0-alpha5"
val typesafeConfigVersion = "1.4.2"
val apacheCommonIOVersion = "2.11.0"
val scalacticVersion = "3.2.9"
val generexVersion = "1.0.2"

resolvers += Resolver.jcenterRepo

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % typesafeConfigVersion,
  "org.scalactic" %% "scalactic" % scalacticVersion,
  "org.scalatest" %% "scalatest" % scalacticVersion % Test,
  "org.scalatest" %% "scalatest-featurespec" % scalacticVersion % Test,
  "com.github.mifmif" % "generex" % generexVersion,
  "org.apache.hadoop" % "hadoop-common" % "3.3.2",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.2",
  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "3.3.2"
)


