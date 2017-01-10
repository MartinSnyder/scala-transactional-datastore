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

package com.martinsnyder.datastore.quill.loose

import com.martinsnyder.datastore.quill.loose
import com.martinsnyder.datastore.{ Condition, EqualsCondition }
import io.getquill.ast.{ Ast, BinaryOperation, Constant, EqualityOperator, Filter, Property }

import scala.annotation.tailrec
import scala.language.{ implicitConversions, reflectiveCalls }
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

object Converters {
  type AstProvider = { def ast: Ast }

  def warm(): Unit = {
    val toolbox = currentMirror.mkToolBox()
    toolbox.eval(toolbox.parse("5"))
  }

  /*
   * Use duck typing because the trait we want to use (Quoted) is not available statically, only
   * as a type member of a Context instance
   */
  implicit def toCondition(astProvider: AstProvider): Condition =
    loose.Converters.toCondition(astProvider.ast)

  @tailrec
  private def toCondition(ast: Ast): Condition =
    ast match {
      case filter: Filter =>
        toCondition(filter.body)

      case BinaryOperation(Property(_, attributeName), EqualityOperator.`==`, Constant(v)) =>
        EqualsCondition(attributeName, v)

      case BinaryOperation(Property(_, attributeName), EqualityOperator.`==`, Property(_, value)) =>
        val toolbox = currentMirror.mkToolBox()
        EqualsCondition(attributeName, toolbox.eval(toolbox.parse(value.toString)))

      case _ =>
        ???
    }
}
