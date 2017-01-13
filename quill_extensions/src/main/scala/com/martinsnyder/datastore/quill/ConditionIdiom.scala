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
import io.getquill.ast.{ Ast, BinaryOperation, Constant, Entity, EqualityOperator, Filter, Property, ScalarValueLift }
import io.getquill.idiom.{ Idiom, ScalarLiftToken, Statement, StringToken, Token }

import scala.annotation.tailrec
import scala.language.implicitConversions

class ConditionIdiom extends Idiom {
  override def liftingPlaceholder(index: Int): String = s"!!$index"

  override def emptyQuery: String = ConditionSerializer.serialize(AllCondition())

  override def prepareForProbing(string: String): String = string

  override def translate(ast: Ast)(implicit naming: NamingStrategy): (Ast, Statement) = {
    ast match {
      case io.getquill.ast.Map(Filter(Entity(typeName, _), _, body), _, _) =>
        val (condition, liftings) = toCondition(ast, Nil)

        val preludeTokens: List[Token] = List(
          StringToken(s"""{ "typeName": "$typeName", "condition": """),
          StringToken(ConditionSerializer.serialize(condition)),
          StringToken(s""", "liftings": [""")
        )

        val liftingTokens: List[Token] = liftings
          .flatMap(svl => List(
            StringToken(","), // Put the delimiting comma in the leading position
            StringToken("\""),
            ScalarLiftToken(svl),
            StringToken("\"")
          )) match {
            case Nil => Nil
            case tokens => tokens.tail // Strip off the "head" of this list, which is the incorrect first comma
          }

        val postludeTokens: List[Token] = List(
          StringToken(s"""] }""")
        )

        (ast, Statement(List(preludeTokens, liftingTokens, postludeTokens).flatten))

      case _ =>
        throw new Exception(s"Unexpected top-level ast: ${ast.getClass.getName}\n${ast.toString}")
    }
  }

  @tailrec
  private def toCondition(ast: Ast, liftedValues: List[ScalarValueLift]): (Condition, List[ScalarValueLift]) =
    ast match {
      case Filter(Entity(className, props), _, body) =>
        toCondition(body, liftedValues)

      case BinaryOperation(Property(_, attributeName), EqualityOperator.`==`, Constant(v)) =>
        (EqualsCondition(attributeName, v), liftedValues)

      case BinaryOperation(Property(_, attributeName), EqualityOperator.`==`, svl: ScalarValueLift) =>
        val placeholder = liftingPlaceholder(liftedValues.size)
        (EqualsCondition(attributeName, placeholder), liftedValues ::: List(svl))

      case map: io.getquill.ast.Map =>
        toCondition(map.query, liftedValues)

      case other =>
        throw new Exception(s"Cannot convert ast to condition: ${other.getClass.getName}\n${other.toString}")
    }
}

object ConditionIdiom extends ConditionIdiom
