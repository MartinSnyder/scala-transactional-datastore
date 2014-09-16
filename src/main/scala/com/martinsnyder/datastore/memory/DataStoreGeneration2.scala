package com.martinsnyder.datastore.memory

import com.martinsnyder.datastore._
import scala.reflect.ClassTag
import scala.util.{Success, Try}

object DataStoreGeneration2 {
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

  // Immutable
  class RecordStore(allRecords: List[Record] = Nil) {
    def createRecords[T <: Record](records: Seq[Record]): Try[RecordStore] = Try({
      new RecordStore(allRecords ::: records.toList)
    })

    def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] =
      Try(filter(allRecords.filter(_.getClass == recordTag.runtimeClass), condition))

    def updateRecord[T <: Record](condition: Condition, record: Record): Try[(RecordStore, Record)] = Try({
      filter(allRecords.filter(_.getClass == record.getClass), condition) match {
        case Seq() =>
          throw new Exception("No records found")

        case Seq(existingRecord) =>
          (new RecordStore(allRecords.filter(_ != existingRecord) ::: List(record)), existingRecord)

        case tooManyRecords =>
          throw new Exception("Too many records")
      }
    })

    def deleteRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[(RecordStore, Seq[Record])] = Try({
      val deletedRecords = filter(allRecords, condition)

      (new RecordStore(allRecords.filter(!deletedRecords.contains(_))), deletedRecords)
    })
  }

  class MutableRecordStore(private var recordStore: RecordStore = new RecordStore()) {
    def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]) = synchronized(
      recordStore.retrieveRecords(condition)
    )

    def createRecords[T <: Record](records: Seq[Record]): Try[Unit] = synchronized(
      for (
        newStore <- recordStore.createRecords(records)
      )
      yield {
        recordStore = newStore
        ()
      }
    )

    def updateRecord[T <: Record](condition: Condition, record: Record): Try[Record] = synchronized(
      for (
        (newStore, existingRecord) <- recordStore.updateRecord(condition, record)
      )
      yield {
        recordStore = newStore
        existingRecord
      }
    )

    def deleteRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] = synchronized(
      for (
        (newStore, deletedRecords) <- recordStore.deleteRecords(condition)
      )
      yield {
        recordStore = newStore
        deletedRecords
      }
    )

    def copy =
      new MutableRecordStore(recordStore)

    def apply(operations: List[DataModification]): Try[Unit] = synchronized({
      val startingRecordStore = recordStore

      val results = operations.map({
        case CreateRecords(records) =>
          createRecords(records)

        case UpdateRecord(oldRecord, newRecord) =>
          updateRecord(ExactMatchCondition(Seq(oldRecord)), newRecord)

        case DeleteRecords(records) =>
          deleteRecords(ExactMatchCondition(records))
      })

      val failures = results.filter(_.isFailure)
      if (failures.isEmpty) {
        Success(())
      }
      else {
        // Restore our starting record store at the beginning of this operation
        recordStore = startingRecordStore
        failures.head.asInstanceOf[Try[Unit]]
      }
    })
  }

  trait DataModification
  case class CreateRecords(records: Seq[Record]) extends DataModification
  case class UpdateRecord(oldRecord: Record, newRecord: Record) extends DataModification
  case class DeleteRecords(records: Seq[Record]) extends DataModification
}

class DataStoreGeneration2 extends DataStore {
  import DataStoreGeneration2._

  private val recordStore = new MutableRecordStore()

  class LocalReadConnection(baseStore: MutableRecordStore) extends ReadConnection {
    override def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]) =
      recordStore.retrieveRecords(condition)

    override def inTransaction[T](f: (WriteConnection) => Try[T]): Try[T] = {
      val writeConnection = new LocalWriteConnection(recordStore.copy)
      for (
        result <- f(writeConnection)
      )
      yield {
        recordStore.apply(writeConnection.modifications)
        result
      }
    }
  }

  class LocalWriteConnection(baseStore: MutableRecordStore) extends LocalReadConnection(baseStore) with WriteConnection {
    val transactionStore = baseStore.copy
    var modifications: List[DataModification] = Nil

    override def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]) =
      transactionStore
        .retrieveRecords(condition)

    override def createRecords[T <: Record](records: Seq[Record]): Try[Unit] =
      transactionStore
        .createRecords(records)
        .map(_ =>
          modifications = CreateRecords(records) :: modifications
        )

    override def updateRecord[T <: Record](condition: Condition, record: Record): Try[Record] =
      transactionStore
        .updateRecord(condition, record)
        .map(oldRecord => {
          modifications = UpdateRecord(oldRecord, record) :: modifications
          oldRecord
        })

    override def deleteRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] =
      transactionStore
        .deleteRecords(condition)
        .map(deletedRecords => {
          modifications = DeleteRecords(deletedRecords) :: modifications
          deletedRecords
        })

    def commit(): Unit = {
      modifications.reverse.foreach({
        case CreateRecords(records) =>

        case UpdateRecord(oldRecord, newRecord) =>

        case DeleteRecords(records) =>

      })
    }
  }

  override def withConnection[T](f: (ReadConnection) => T): T =
    f(new LocalReadConnection(recordStore))
}
