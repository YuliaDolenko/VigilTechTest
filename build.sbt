ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "VigilTechTest",
    version := "0.1.0-SNAPSHOT",
  )

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.15" % "test",
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "com.amazonaws" % "aws-java-sdk" % "1.11.698",
  "org.apache.hadoop" % "hadoop-common" % "3.3.2" exclude("com.google.guava", "guava"),
  "org.apache.hadoop" % "hadoop-client" % "3.3.2" exclude("com.google.guava", "guava"),
  "org.apache.hadoop" % "hadoop-aws" % "3.3.2" exclude("com.google.guava", "guava"),
  "com.google.guava" % "guava" % "30.1-jre")
