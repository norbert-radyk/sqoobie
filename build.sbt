name := "sqoobie"

description := "An attempt to re-purpose type-safe Squeryl query DSL for Doobie engine"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.13.8"

organization := "sqoobie"

licenses := Seq("Apache 2" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))

parallelExecution := false

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
)

libraryDependencies ++= Seq(
  "cglib" % "cglib-nodep" % "3.3.0",
  "com.h2database" % "h2" % "1.4.200" % "provided",
  "mysql" % "mysql-connector-java" % "8.0.27" % "provided",
  "org.postgresql" % "postgresql" % "42.3.1" % "provided",
  "net.sourceforge.jtds" % "jtds" % "1.3.1" % "provided",
  "org.apache.derby" % "derby" % "10.15.2.0" % "provided",
  "org.json4s" %% "json4s-scalap" % "3.6.12",
  "org.scala-lang.modules" %% "scala-xml" % "2.0.1",
  "org.xerial" % "sqlite-jdbc" % "3.36.0.3" % "test",
  "org.scalatest" %% "scalatest" % "3.2.10" % "test"
)

lazy val sqoobie = (project in file("."))
  .settings(
    name := "sqoobie"
  )
