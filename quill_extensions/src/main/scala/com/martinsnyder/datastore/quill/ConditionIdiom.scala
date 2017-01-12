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

import com.martinsnyder.datastore.{ AllCondition, Condition, EqualsCondition }
import io.getquill.NamingStrategy
import io.getquill.ast.{ Ast, BinaryOperation, Constant, EqualityOperator, Filter, Property }
import io.getquill.idiom.{ Idiom, Statement, StringToken }

import scala.annotation.tailrec
import scala.language.implicitConversions

class ConditionIdiom extends Idiom {
  // Format for String.format
  override def liftingPlaceholder(index: Int): String = "%" + index + "s"

  override def emptyQuery: String = ConditionSerializer.serialize(AllCondition())

  override def prepareForProbing(string: String): String = string

  override def translate(ast: Ast)(implicit naming: NamingStrategy): (Ast, Statement) = {
    val condition = toCondition(ast)

    (ast, Statement(List(StringToken(ConditionSerializer.serialize(condition)))))
  }

  @tailrec
  private def toCondition(ast: Ast): Condition =
    ast match {
      case filter: Filter =>
        toCondition(filter.body)

      case BinaryOperation(Property(_, attributeName), EqualityOperator.`==`, Constant(v)) =>
        EqualsCondition(attributeName, v)

      case map: io.getquill.ast.Map =>
        println(map: io.getquill.ast.Map)
        toCondition(map.query)

      case _ =>
        ???
    }
}

object ConditionIdiom extends ConditionIdiom