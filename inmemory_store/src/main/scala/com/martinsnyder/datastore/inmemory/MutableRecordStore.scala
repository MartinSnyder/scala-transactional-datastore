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

import com.martinsnyder.datastore.{ Condition, Record }

import scala.reflect.ClassTag
import scala.util.Try

class MutableRecordStore(private var recordStore: RecordStore) {
  /**
   * Add records to the store
   */
  def createRecords[T <: Record](records: Seq[Record]): Try[Unit] =
    for (
      nextRecordStore <- recordStore.createRecords(records)
    ) yield recordStore = nextRecordStore

  /**
   * Load records from the store.
   */
  def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[T]] =
    recordStore.retrieveRecords(condition)

  /**
   * Update a single existing record.  If condition does not resolve to exactly one record, then
   * an exception will be thrown.  Returns previous copy of the record
   */
  def updateRecord[T <: Record](condition: Condition, record: Record): Try[Record] =
    for (
      (nextRecordStore, updatedRecord) <- recordStore.updateRecord(condition, record)
    ) yield {
      recordStore = nextRecordStore
      updatedRecord
    }

  /**
   * Remove records from the store that match condition.  Returns the records that were removed.
   */
  def deleteRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] =
    for (
      (nextRecordStore, deletedRecords) <- recordStore.deleteRecords(condition)
    ) yield {
      recordStore = nextRecordStore
      deletedRecords
    }

  def copy: MutableRecordStore =
    new MutableRecordStore(recordStore)

  def applyOperations(transactionLog: List[Operation]): Try[Unit] =
    for (
      nextRecordStore <- recordStore.applyOperations(transactionLog)
    ) yield {
      recordStore = nextRecordStore
    }
}
