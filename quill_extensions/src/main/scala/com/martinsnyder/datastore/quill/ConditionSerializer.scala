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

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{ JsonSubTypes, JsonTypeInfo }
import com.fasterxml.jackson.databind.Module.SetupContext
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.martinsnyder.datastore.{ AllCondition, Condition, EqualsCondition, ExactMatchCondition }

object ConditionSerializer {
  private object MixinModule extends SimpleModule {
    override def setupModule(context: SetupContext): Unit = {
      context.setMixInAnnotations(classOf[Condition], classOf[ConditionSerializationMixin])
    }
  }

  private val json = new ObjectMapper()
  json.registerModule(DefaultScalaModule)
  json.registerModule(MixinModule)

  @JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
  )
  @JsonSubTypes(Array(
    new Type(value = classOf[AllCondition], name = "all"),
    new Type(value = classOf[EqualsCondition[_]], name = "eq"),
    new Type(value = classOf[ExactMatchCondition], name = "exact")
  ))
  private abstract class ConditionSerializationMixin

  def serialize(condition: Condition): String = json.writeValueAsString(condition)

  def deserialize(condition: String): Condition = json.readValue(condition, classOf[Condition])

  def deserializeQuery(query: String): DataStoreQuery = json.readValue(query, classOf[DataStoreQuery])
}

