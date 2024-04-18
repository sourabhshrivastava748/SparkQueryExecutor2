ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % "3.5.0",
    "org.scalatest" %% "scalatest" % "3.2.15" % "test"
)

lazy val root = (project in file("."))
  .settings(
    name := "SparkQueryExecutor2"
  )
