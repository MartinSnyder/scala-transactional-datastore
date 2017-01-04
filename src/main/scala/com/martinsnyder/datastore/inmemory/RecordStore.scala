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

import com.martinsnyder.datastore.{ AllCondition, Condition, EqualsCondition, ExactMatchCondition, Record }

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Try

object RecordStore {
  implicit def conditionToPredicate(condition: Condition): Record => Boolean = {
    condition match {
      case AllCondition =>
        _ => true

      case ExactMatchCondition(matchRecords) =>
        r => {
          matchRecords.contains(r)
        }

      case EqualsCondition(fieldName, value) =>
        _.getFieldValue(fieldName) == value
    }
  }
}

class RecordStore(storedRecords: List[Record], enforcers: List[ConstraintEnforcer]) {
  import RecordStore.conditionToPredicate

  /**
   * Add records to the store
   */
  def createRecords[T <: Record](records: Seq[Record]): Try[RecordStore] =
    for (
      newEnforcers <- applyConstraints(_.createRecords(records))
    ) yield new RecordStore(storedRecords ::: records.toList, newEnforcers)

  /**
   * Load records from the store.
   */
  def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] =
    queryConstraints[T](condition).map({
      case None =>
        storedRecords.filter(condition)

      case Some(results) =>
        results
    })

  /**
   * Update a single existing record.  If condition does not resolve to exactly one record, then
   * an exception will be thrown.  Returns previous copy of the record
   */
  def updateRecord[T <: Record](condition: Condition, record: Record): Try[(RecordStore, Record)] =
    for (
      (newRecords, oldRecord) <- Try(
        storedRecords.filter(condition) match {
          case Nil =>
            throw new Exception("Could not find record)")

          case Seq(recordToUpdate) =>
            (record :: storedRecords.filter(_ != recordToUpdate), recordToUpdate)

          case biggerSequence =>
            throw new Exception(s"Too many records ${biggerSequence.length}")
        }
      );

      newEnforcers <- applyConstraints(_.updateRecord(oldRecord, record))
    ) yield (new RecordStore(newRecords, newEnforcers), oldRecord)

  /**
   * Remove records from the store that match condition.  Returns the records that were removed.
   */
  def deleteRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[(RecordStore, Seq[Record])] =
    for (
      (applicableRecords, otherRecords) <- Try(storedRecords.partition(_.getClass == recordTag.runtimeClass));
      (recordsToDelete, recordsToKeep) <- Try(applicableRecords.partition(condition));

      newEnforcers <- applyConstraints(_.deleteRecords(recordsToDelete))
    ) yield (new RecordStore(otherRecords ::: recordsToKeep, newEnforcers), recordsToDelete)

  def applyOperations(transactionLog: List[Operation]): Try[RecordStore] = {
    def privateCombine(maybeStore: Try[RecordStore], op: Operation): Try[RecordStore] = {
      maybeStore.flatMap(store => {
        op match {
          case CreateOp(records) =>
            store.createRecords(records)

          case UpdateOp(oldRecord, newRecord) =>
            store.updateRecord(ExactMatchCondition(List(oldRecord)), newRecord)
              .map(_._1)

          case DeleteOp(deletedRecords) =>
            store.deleteRecords(ExactMatchCondition(deletedRecords))(ClassTag(deletedRecords.head.getClass))
              .map(_._1)
        }
      })
    }

    transactionLog.foldLeft(Try(this))(privateCombine)
  }

  private def applyConstraints(action: ConstraintEnforcer => Try[ConstraintEnforcer]): Try[List[ConstraintEnforcer]] = {
    def privateApply(maybeProgress: Try[List[ConstraintEnforcer]], next: ConstraintEnforcer): Try[List[ConstraintEnforcer]] = {
      maybeProgress.flatMap(progress => {
        val stuff = action(next).map(_ :: progress)
        stuff
      })
    }

    val result = enforcers.foldLeft(Try(List[ConstraintEnforcer]()))(privateApply)
    result
  }

  private def queryConstraints[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Option[Seq[T]]] = {
    def privateApply(maybeProgress: Try[Option[Seq[T]]], next: ConstraintEnforcer): Try[Option[Seq[T]]] =
      maybeProgress.flatMap({
        case None =>
          next.retrieve(recordTag.runtimeClass.getName, condition).map(_.map(_.map(_.asInstanceOf[T])))

        case results =>
          Try(results)
      })

    enforcers.foldLeft(Try(None: Option[Seq[T]]))(privateApply)
  }
}
