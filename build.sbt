name := "Scala Transactional Data Store"

organization in ThisBuild := "com.martinsnyder"

version in ThisBuild := "0.0.1"

scalaVersion in ThisBuild := "2.11.8"

scalacOptions in ThisBuild ++= Seq("-feature", "-deprecation")

libraryDependencies ++= Seq(
  "io.getquill" %% "quill-jdbc" % "1.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
