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

import com.martinsnyder.datastore.{ Condition, Constraint, DataStore, ReadConnection, Record, WriteConnection }

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.Try

class InMemoryDataStore(constraints: Seq[Constraint]) extends DataStore {
  val recordStore = new MutableRecordStore(new RecordStore(Nil, constraints.map(ConstraintEnforcer.apply).toList))

  override def withConnection[T](f: (ReadConnection) => T): T =
    f(new PLReadConnection)

  class PLReadConnection extends ReadConnection {
    /**
     * Load records from the store.
     */
    override def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] =
      recordStore.retrieveRecords(condition)

    /**
     * Perform write operations in a transaction
     */
    override def inTransaction[T](f: (WriteConnection) => Try[T]): Try[T] = {
      val txn = new PLWriteConnection(recordStore)
      for (
        result <- f(txn);
        _ <- txn.commit
      ) yield result
    }
  }

  class PLWriteConnection(initialRecordStore: MutableRecordStore) extends WriteConnection {
    private val transactionStore = initialRecordStore.copy
    private var transactionLog: List[Operation] = Nil

    override def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] =
      transactionStore.retrieveRecords(condition)

    override def inTransaction[T](f: (WriteConnection) => Try[T]): Try[T] = {
      val txn = new PLWriteConnection(transactionStore)
      for (
        result <- f(txn);
        _ <- txn.commit
      ) yield result
    }

    /**
     * Add records to the store
     */
    override def createRecords[T <: Record](records: Seq[Record]): Try[Unit] =
      for (
        _ <- transactionStore.createRecords(records)
      ) yield {
        transactionLog = CreateOp(records) :: transactionLog
      }

    /**
     * Update a single existing record.  If condition does not resolve to exactly one record, then
     * an exception will be thrown.  Returns previous copy of the record
     */
    override def updateRecord[T <: Record](condition: Condition, record: Record): Try[Record] =
      for (
        updatedRecord <- transactionStore.updateRecord(condition, record)
      ) yield {
        transactionLog = UpdateOp(updatedRecord, record) :: transactionLog
        updatedRecord
      }

    /**
     * Remove records from the store that match condition.  Returns the records that were removed.
     */
    override def deleteRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] =
      for (
        deletedRecords <- transactionStore.deleteRecords(condition)
      ) yield {
        transactionLog = DeleteOp(deletedRecords) :: transactionLog
        deletedRecords
      }

    def commit: Try[Unit] =
      initialRecordStore.applyOperations(transactionLog.reverse)
  }
}
