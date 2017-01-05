/*
    The MIT License (MIT)

    Copyright (c) 2014 Martin Snyder

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

name := "Scala Transactional Data Store"

organization in ThisBuild := "com.martinsnyder"

version in ThisBuild := "0.0.1"

scalaVersion in ThisBuild := "2.11.8"

scalacOptions in ThisBuild ++= Seq("-feature", "-deprecation")

lazy val inmemory_store = project.in(file("inmemory_store"))
  .settings(libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  ))

lazy val quill_extensions = project.in(file("quill_extensions"))
  .dependsOn(inmemory_store)
  .settings(libraryDependencies ++= Seq(
    "io.getquill" %% "quill-core" % "1.0.1",
    "org.scala-lang" % "scala-compiler" % "2.11.8",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  ))
