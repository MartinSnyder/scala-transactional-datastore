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

package com.martinsnyder.datastore.quill

import com.martinsnyder.datastore.quill.Data.Person
import com.martinsnyder.datastore.{ Condition, EqualsCondition }
import io.getquill._
import io.getquill.ast.{ Ast, BinaryOperation, EqualityOperator, Filter, Property }

import scala.language.implicitConversions
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

object QuillDemo {
  type QuillContext = MirrorContext[MirrorIdiom, Literal]

  def astToCondition(ast: Ast): Condition =
    ast match {
      case BinaryOperation(Property(_, attributeName), EqualityOperator.`==`, Property(_, value)) =>
        val toolbox = currentMirror.mkToolBox()
        EqualsCondition(attributeName, toolbox.eval(toolbox.parse(value.toString)))

      case _ =>
        ???
    }

  def main(args: Array[String]): Unit = {
    val ctx = new QuillContext

    import ctx._

    implicit def toCondition(q: ctx.Quoted[_]): Condition = {
      q.ast match {
        case filter: Filter =>
          astToCondition(filter.body)

        case _ =>
          ???
      }
    }

    val dataStore = Data.sampleDataStore

    val unemployed =
      dataStore
        .withConnection(_.retrieveRecords[Person](EqualsCondition("occupation", None)))
        .map(_.map(person => s"${person.givenName} ${person.familyName}"))

    val quillUnemployed =
      dataStore
        .withConnection(_.retrieveRecords[Person](quote { query[Person].filter(_.occupation == None) }))
        .map(_.map(person => s"${person.givenName} ${person.familyName}"))

    println(unemployed)
    println(quillUnemployed)
  }
}
