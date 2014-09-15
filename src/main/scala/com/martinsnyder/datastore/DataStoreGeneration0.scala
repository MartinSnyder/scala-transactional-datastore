package com.martinsnyder.datastore

import scala.reflect.ClassTag
import scala.util.Try

object DataStoreGeneration0 {
  def getFieldValue(record: Record, fieldName: String) =
    record.getClass.getMethod(fieldName).invoke(record)

  def conditionToPredicate(condition: Condition) = condition match {
    case EqualsCondition(fieldName, value) =>
      (record: Record) => getFieldValue(record, fieldName) == value
  }

  def filter(records: List[Record], condition: Condition) =
    records.filter(conditionToPredicate(condition))
}

class DataStoreGeneration0 extends DataStore {
  import com.martinsnyder.datastore.DataStoreGeneration0._

  var allRecords: List[Record] = Nil

  class LocalReadConnection extends ReadConnection {
    override def loadRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] =
      Try(filter(allRecords.filter(_.isInstanceOf[T]), condition))

    override def inTransaction[T](f: (WriteConnection) => Try[T]): Try[T] =
      f(new LocalWriteConnection)
  }

  class LocalWriteConnection extends LocalReadConnection with WriteConnection {
    override def createRecords[T <: Record](records: Seq[Record]): Try[Unit] = Try({
      allRecords = allRecords ::: records.toList
    })

    override def updateRecord[T <: Record](condition: Condition, record: Record): Try[Record] = Try({
      filter(allRecords.filter(_.isInstanceOf[T]), condition) match {
        case Seq() =>
          throw new Exception("No records found")

        case Seq(existingRecord) =>
          allRecords = allRecords.filter(_ != existingRecord) ::: List(record)
          existingRecord

        case tooManyRecords =>
          throw new Exception("Too many records")
      }
    })

    override def deleteRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] = Try({
      val deletedRecords = filter(allRecords, condition)

      allRecords = allRecords.filter(!deletedRecords.contains(_))

      deletedRecords
    })
  }

  override def withConnection[T](f: (ReadConnection) => T): T =
    f(new LocalReadConnection)
}
