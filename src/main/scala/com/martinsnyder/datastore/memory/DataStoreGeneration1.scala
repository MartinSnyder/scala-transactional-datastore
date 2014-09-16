package com.martinsnyder.datastore.memory

import com.martinsnyder.datastore._

import scala.reflect.ClassTag
import scala.util.Try

object DataStoreGeneration1 {
  def getFieldValue(record: Record, fieldName: String) =
    record.getClass.getMethod(fieldName).invoke(record)

  def conditionToPredicate(condition: Condition) = condition match {
    case AllCondition =>
      (record: Record) => true

    case EqualsCondition(fieldName, value) =>
      (record: Record) => getFieldValue(record, fieldName) == value

    case ExactMatchCondition(matchRecords) =>
      (record: Record) => matchRecords.contains(record)
  }

  def filter(records: List[Record], condition: Condition) =
    records.filter(conditionToPredicate(condition))
}

class DataStoreGeneration1 extends DataStore {
  import DataStoreGeneration1._

  var allRecords: List[Record] = Nil

  class LocalReadConnection extends ReadConnection {
    override def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] = {
      assert(recordTag.runtimeClass != classOf[Nothing], "Generic Type must be specified to loadRecords")

      Try(filter(allRecords.filter(_.getClass == recordTag.runtimeClass), condition))
    }

    override def inTransaction[T](f: (WriteConnection) => Try[T]): Try[T] =
      f(new LocalWriteConnection)
  }

  class LocalWriteConnection extends LocalReadConnection with WriteConnection {
    override def createRecords[T <: Record](records: Seq[Record]): Try[Unit] = Try({
      allRecords = allRecords ::: records.toList
    })

    override def updateRecord[T <: Record](condition: Condition, record: Record): Try[Record] = Try({
      filter(allRecords.filter(_.getClass == record.getClass), condition) match {
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
