name := "scala-transactional-datastore"

organization in ThisBuild := "com.martinsnyder"

version in ThisBuild := "0.0.1"

scalaVersion in ThisBuild := "2.11.0"

scalacOptions in ThisBuild ++= Seq("-feature", "-deprecation")

ideaExcludeFolders ++= Seq(".idea", ".idea_modules")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.1.6" % "test"
)
