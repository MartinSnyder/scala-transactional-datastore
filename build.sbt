name := "scala-transactional-datastore"

organization in ThisBuild := "com.martinsnyder"

version in ThisBuild := "0.0.1"

scalaVersion in ThisBuild := "2.12.1"

scalacOptions in ThisBuild ++= Seq("-feature", "-deprecation")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
