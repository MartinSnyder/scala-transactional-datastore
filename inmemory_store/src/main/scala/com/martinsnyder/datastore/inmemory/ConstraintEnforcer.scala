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

package com.martinsnyder.datastore.inmemory

import com.martinsnyder.datastore.DataStore.ConstraintViolation
import com.martinsnyder.datastore.{ Condition, Constraint, EqualsCondition, ForeignKeyConstraint, ImmutableConstraint, Record, UniqueConstraint }

import scala.util.{ Success, Try }

object ConstraintEnforcer {
  def apply(constraint: Constraint): ConstraintEnforcer = constraint match {
    case uc: UniqueConstraint => new UniqueConstraintEnforcer(uc, Map())
    case ic: ImmutableConstraint => new ImmutableConstraintEnforcer(ic)
    case fkc: ForeignKeyConstraint => new ForeignKeyConstraintEnforcer(fkc, Map(), ConstraintEnforcer(fkc.target))
  }
}

sealed trait ConstraintEnforcer {
  def createRecords[T <: Record](records: Seq[Record]): Try[ConstraintEnforcer]
  def updateRecord[T <: Record](oldRecord: Record, newRecord: Record): Try[ConstraintEnforcer]
  def deleteRecords[T <: Record](records: Seq[Record]): Try[ConstraintEnforcer]
  def retrieve(className: String, condition: Condition): Try[Option[Seq[Record]]] = Try(None)
}

class ForeignKeyConstraintEnforcer(constraint: ForeignKeyConstraint, currentValues: Map[AnyRef, Int], targetValues: ConstraintEnforcer) extends ConstraintEnforcer {
  override def createRecords[T <: Record](records: Seq[Record]): Try[ForeignKeyConstraintEnforcer] = {
    val applicableRecords = records.filter(_.getClass.getName == constraint.sourceClassName)
    val incomingValues = applicableRecords.map(_.getFieldValue(constraint.sourceFieldName))

    // Verify that all these incoming values are legit
    val targetRecords = incomingValues.map(value =>
      targetValues.retrieve(constraint.target.className, EqualsCondition(constraint.target.fieldName, value)) match {
        case Success(Some(Seq(record))) =>
          Some(record)

        case _ =>
          None
      })

    // Pointer to a non-existent target
    if (targetRecords.exists(_.isEmpty)) {
      Try(throw ConstraintViolation(constraint))
    } else {
      val updatedValues = incomingValues.foldLeft(currentValues)((progress, nextVal) =>
        progress.get(nextVal) match {
          case Some(count) =>
            progress + (nextVal -> (count + 1))
          case None =>
            progress + (nextVal -> 1)
        })

      for (
        updatedTargetValues <- targetValues.createRecords(records)
      ) yield new ForeignKeyConstraintEnforcer(constraint, updatedValues, updatedTargetValues)
    }
  }

  override def updateRecord[T <: Record](oldRecord: Record, newRecord: Record): Nothing = ???

  override def deleteRecords[T <: Record](records: Seq[Record]): Try[ForeignKeyConstraintEnforcer] =
    records.headOption.map(_.getClass.getName) match {
      case None =>
        Try(this)

      case Some(className) =>
        if (className == constraint.target.className) {
          val values = records.map(_.getFieldValue(constraint.target.fieldName))
          val occurrences = values.map(currentValues.getOrElse(_, 0: Int)).sum

          if (occurrences > 0) {
            Try(throw ConstraintViolation(constraint))
          } else {
            for (
              updatedTargetValues <- targetValues.deleteRecords(records)
            ) yield new ForeignKeyConstraintEnforcer(constraint, currentValues, updatedTargetValues)
          }
        } else {
          Try(this)
        }
    }
}

class UniqueConstraintEnforcer(constraint: UniqueConstraint, uniqueValues: Map[AnyRef, Record]) extends ConstraintEnforcer {
  override def createRecords[T <: Record](records: Seq[Record]): Try[UniqueConstraintEnforcer] = Try({
    val applicableRecords = records.filter(_.getClass.getName == constraint.className)
    val incomingValues = applicableRecords.map(record => record.getFieldValue(constraint.fieldName) -> record)

    val updatedValues = uniqueValues ++ incomingValues.toMap
    if (updatedValues.size != (uniqueValues.size + applicableRecords.size)) {
      throw ConstraintViolation(constraint)
    }

    new UniqueConstraintEnforcer(constraint, updatedValues)
  })

  override def updateRecord[T <: Record](oldRecord: Record, newRecord: Record): Try[UniqueConstraintEnforcer] = Try(
    if (oldRecord.getClass.getName == newRecord.getClass.getName && newRecord.getClass.getName == constraint.className) {
      val oldValue = oldRecord.getFieldValue(constraint.fieldName)
      val newValue = newRecord.getFieldValue(constraint.fieldName)

      val updatedValues = uniqueValues - oldValue + (newValue -> newRecord)
      if (uniqueValues.size != updatedValues.size) {
        throw ConstraintViolation(constraint)
      }

      new UniqueConstraintEnforcer(constraint, updatedValues)

      this
    } else {
      this
    }
  )

  override def deleteRecords[T <: Record](records: Seq[Record]): Try[UniqueConstraintEnforcer] = Try({
    val applicableRecords = records.filter(_.getClass.getName == constraint.className)
    val outgoingValues = applicableRecords.map(_.getFieldValue(constraint.fieldName))

    val updatedValues = uniqueValues -- outgoingValues
    if (updatedValues.size != (uniqueValues.size - applicableRecords.size)) {
      throw ConstraintViolation(constraint)
    }

    new UniqueConstraintEnforcer(constraint, updatedValues)
  })

  override def retrieve(className: String, condition: Condition): Try[Option[Seq[Record]]] = Try(
    condition match {
      case EqualsCondition(conditionField, conditionValue: AnyRef) =>
        if (className == constraint.className && conditionField == constraint.fieldName) {
          uniqueValues.get(conditionValue).map(record => List(record))
        } else {
          None
        }

      case _ =>
        None
    }
  )
}

class ImmutableConstraintEnforcer(constraint: ImmutableConstraint) extends ConstraintEnforcer {
  override def createRecords[T <: Record](records: Seq[Record]): Try[ImmutableConstraintEnforcer] =
    Try(this)

  override def updateRecord[T <: Record](oldRecord: Record, newRecord: Record): Try[Nothing] =
    Try(throw ConstraintViolation(constraint))

  override def deleteRecords[T <: Record](records: Seq[Record]): Try[Nothing] =
    Try(throw ConstraintViolation(constraint))
}
