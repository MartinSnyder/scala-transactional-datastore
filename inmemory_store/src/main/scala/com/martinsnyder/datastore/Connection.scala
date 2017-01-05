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

package com.martinsnyder.datastore

import scala.util.Try
import scala.reflect.ClassTag

sealed trait Connection

// Read connection to a data store.  Provides access to write operations via inTransaction
trait ReadConnection extends Connection {
  /**
   * Load records from the store.
   */
  def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[T]]

  /**
   * Perform write operations in a transaction
   */
  def inTransaction[T](f: WriteConnection => Try[T]): Try[T]
}

// Write connection to a data store.  Only available within a transaction.
trait WriteConnection extends ReadConnection {
  /**
   * Add records to the store
   */
  def createRecords[T <: Record](records: Seq[Record]): Try[Unit]

  /**
   * Update a single existing record.  If condition does not resolve to exactly one record, then
   * an exception will be thrown.  Returns previous copy of the record
   */
  def updateRecord[T <: Record](condition: Condition, record: Record): Try[Record]

  /**
   * Remove records from the store that match condition.  Returns the records that were removed.
   */
  def deleteRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]]
}
