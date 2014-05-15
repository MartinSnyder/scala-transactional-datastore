package com.martinsnyder.datastore

import scala.util.Try
import scala.reflect.ClassTag

sealed trait Connection

trait ReadConnection extends Connection {
  def loadRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]]
  def inTransaction[T](f: WriteConnection => Try[T]): Try[T]
}

trait WriteConnection extends ReadConnection {
  def insertRecords[T <: Record](records: Seq[Record]): Try[Unit]
  def deleteRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]]
}
