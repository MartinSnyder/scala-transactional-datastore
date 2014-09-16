package com.martinsnyder.datastore

import scala.util.Try
import scala.reflect.ClassTag

sealed trait Connection

// Read connection to a data store.  Provides access to write operations via inTransaction
trait ReadConnection extends Connection {
  /**
   * Load records from the store.
   */
  def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]]

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
