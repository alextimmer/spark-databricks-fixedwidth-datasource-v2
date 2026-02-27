ThisBuild / organization := "com.alexandertimmer"
ThisBuild / scalaVersion := "2.13.12"
ThisBuild / description  := "A Spark 4 DataSource V2 implementation for reading fixed-width files"
ThisBuild / homepage     := Some(url("https://github.com/AlexanderTimmer/spark-fixedwidth-datasource"))
ThisBuild / licenses     += "Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt")

version := "0.1.0-SNAPSHOT"

val sparkVersion = "4.0.2"

lazy val root = (project in file("."))
  .settings(
    name := "spark-fixedwidth-datasource",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.17.0",
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
    ),

    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-encoding", "utf8"
    ),

    Test / parallelExecution := false
  )
