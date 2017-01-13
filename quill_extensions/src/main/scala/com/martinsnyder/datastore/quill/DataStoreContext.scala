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

import java.time.LocalDate
import java.util.Date

import com.martinsnyder.datastore.{ Condition, DataStore, EqualsCondition, Record }
import io.getquill.Literal
import io.getquill.context.Context

import scala.reflect.ClassTag
import scala.util.Try

class DataStoreContext(dataStore: DataStore, recordClasses: Traversable[Class[_ <: Record]])
    extends Context[ConditionIdiom, Literal]
    with Encoders
    with Decoders {

  val recordTypes: Map[String, Class[_ <: Record]] = recordClasses
    .map(recordClass => recordClass.getSimpleName -> recordClass)
    .toMap

  type PrepareRow = Condition
  type ResultRow = Record

  type RunQueryResult[T] = List[T]
  type RunQuerySingleResult[T] = T
  type RunActionResult = Long
  type RunActionReturningResult[T] = T
  type RunBatchActionResult = List[Long]
  type RunBatchActionReturningResult[T] = List[T]

  def probe(statement: String): Try[_] = Try {
    // Probing is purposeless because our in-memory data model is no different from the compile time model
  }

  def close(): Unit = {
    // Data store doesn't need to be closed
  }

  def executeQuery[T](
    queryJson: String,
    prepare: PrepareRow => PrepareRow = identity,
    extractor: Record => T = identity[Record] _
  ): List[T] = {
    val compiledQuery = ConditionSerializer.deserializeQuery(queryJson)
    val runtimeQuery = compiledQuery.copy(condition = prepare(compiledQuery.condition))
    val recordClass = recordTypes.getOrElse(runtimeQuery.typeName, throw new Exception(s"Cannot map ${runtimeQuery.typeName} to a DataStore class"))

    dataStore
      .withConnection(_.retrieveRecords(runtimeQuery.condition)(ClassTag(recordClass)))
      .get
      .toList
      .map(_.asInstanceOf[T])
  }
}

trait Encoders { this: DataStoreContext =>
  type Encoder[T] = DataStoreEncoder[T]

  case class DataStoreEncoder[T]() extends BaseEncoder[T] {
    override def apply(index: Int, value: T, condition: Condition): Condition = {
      condition match {
        case EqualsCondition(fieldName, placeholder) if placeholder == s"!!$index" =>
          // Replace lift placeholder
          EqualsCondition(fieldName, value)

        case _ =>
          // Return unmodified
          condition
      }
    }
  }

  implicit def mappedEncoder[I, O](implicit mapped: MappedEncoding[I, O], e: Encoder[O]): Encoder[I] = throw new NotImplementedError

  implicit def optionEncoder[T](implicit d: Encoder[T]): Encoder[Option[T]] = DataStoreEncoder()
  implicit val stringEncoder: Encoder[String] = DataStoreEncoder()
  implicit val bigDecimalEncoder: Encoder[BigDecimal] = DataStoreEncoder()
  implicit val booleanEncoder: Encoder[Boolean] = DataStoreEncoder()
  implicit val byteEncoder: Encoder[Byte] = DataStoreEncoder()
  implicit val shortEncoder: Encoder[Short] = DataStoreEncoder()
  implicit val intEncoder: Encoder[Int] = DataStoreEncoder()
  implicit val longEncoder: Encoder[Long] = DataStoreEncoder()
  implicit val floatEncoder: Encoder[Float] = DataStoreEncoder()
  implicit val doubleEncoder: Encoder[Double] = DataStoreEncoder()
  implicit val byteArrayEncoder: Encoder[Array[Byte]] = DataStoreEncoder()
  implicit val dateEncoder: Encoder[Date] = DataStoreEncoder()
  implicit val localDateEncoder: Encoder[LocalDate] = DataStoreEncoder()
}

trait Decoders { this: DataStoreContext =>
  type Decoder[T] = DataStoreDecoder[T]

  // We will never invoke our decoders for DataStore records because they are already in the correct format
  case class DataStoreDecoder[T]() extends BaseDecoder[T] {
    override def apply(index: Index, record: Record): T = {
      throw new NotImplementedError
    }
  }

  implicit def mappedDecoder[I, O](implicit mapped: MappedEncoding[I, O], d: Decoder[I]): Decoder[O] = throw new NotImplementedError

  implicit def optionDecoder[T](implicit d: Decoder[T]): Decoder[Option[T]] = DataStoreDecoder()
  implicit val stringDecoder: Decoder[String] = DataStoreDecoder()
  implicit val bigDecimalDecoder: Decoder[BigDecimal] = DataStoreDecoder()
  implicit val booleanDecoder: Decoder[Boolean] = DataStoreDecoder()
  implicit val byteDecoder: Decoder[Byte] = DataStoreDecoder()
  implicit val shortDecoder: Decoder[Short] = DataStoreDecoder()
  implicit val intDecoder: Decoder[Int] = DataStoreDecoder()
  implicit val longDecoder: Decoder[Long] = DataStoreDecoder()
  implicit val floatDecoder: Decoder[Float] = DataStoreDecoder()
  implicit val doubleDecoder: Decoder[Double] = DataStoreDecoder()
  implicit val byteArrayDecoder: Decoder[Array[Byte]] = DataStoreDecoder()
  implicit val dateDecoder: Decoder[Date] = DataStoreDecoder()
  implicit val localDateDecoder: Decoder[LocalDate] = DataStoreDecoder()
}
