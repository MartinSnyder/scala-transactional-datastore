package com.martinsnyder.datastore.memory

import com.martinsnyder.datastore._
import scala.reflect.ClassTag
import scala.util.Try
import scala.language.implicitConversions

// Live coded for Philly Lambda on 09-Oct-2014
// http://www.meetup.com/Philly-Lambda/events/210473142/
// NOTE: This implementation does not include constraints
object PhillyLambdaDataStore {
  def getFieldValue(record: Record, fieldName: String) =
    record.getClass.getMethod(fieldName).invoke(record)

  def conditionToPredicate(condition: Condition): Record => Boolean = {
    condition match {
      case AllCondition =>
        _ => true

      case ExactMatchCondition(matchRecords) =>
        matchRecords.contains

      case EqualsCondition(fieldName, value) =>
        getFieldValue(_, fieldName) == value
    }
  }

  def filter(records: List[Record], condition: Condition) =
    records.filter(conditionToPredicate(condition))

  class ListWrapper(records: List[Record]) {
    def filter(condition: Condition) =
      records.filter(conditionToPredicate(condition))

    def partition(condition: Condition) =
      records.partition(conditionToPredicate(condition))
  }
  implicit def doWrapList(records: List[Record]): ListWrapper = new ListWrapper(records)

  class RecordStore(storedRecords: List[Record]) {
    /**
     * Add records to the store
     */
    def createRecords[T <: Record](records: Seq[Record]): Try[RecordStore] = Try({
      new RecordStore(storedRecords ::: records.toList)
    })

    /**
     * Load records from the store.
     */
    def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] = Try({
      storedRecords
        .filter(condition)
    })

    /**
     * Update a single existing record.  If condition does not resolve to exactly one record, then
     * an exception will be thrown.  Returns previous copy of the record
     */
    def updateRecord[T <: Record](condition: Condition, record: Record): Try[(RecordStore, Record)] = Try({
      storedRecords.filter(condition) match {
        case Nil =>
          throw new Exception("Could not find record)")

        case Seq(recordToUpdate) =>
          (new RecordStore(record :: storedRecords.filter(_ != recordToUpdate)), recordToUpdate)

        case biggerSequence =>
          throw new Exception(s"Too many records ${biggerSequence.length}")
      }
    })

    /**
     * Remove records from the store that match condition.  Returns the records that were removed.
     */
    def deleteRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[(RecordStore, Seq[Record])] =
      for (
        (applicableRecords, otherRecords) <- Try(storedRecords.partition(_.getClass == recordTag.runtimeClass));
        (recordsToDelete, recordsToKeep) <- Try(applicableRecords.partition(condition))
      ) yield (new RecordStore(otherRecords ::: recordsToKeep), recordsToDelete)

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
  }

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
    def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] =
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

    def copy =
      new MutableRecordStore(recordStore)

    def applyOperations(transactionLog: List[Operation]): Try[Unit] =
      for (
        nextRecordStore <- recordStore.applyOperations(transactionLog)
      ) yield {
        recordStore = nextRecordStore
      }
  }

  sealed trait Operation
  case class CreateOp(records: Seq[Record]) extends Operation
  case class UpdateOp(oldRecord: Record, newRecord: Record) extends Operation
  case class DeleteOp(records: Seq[Record]) extends Operation
}

class PhillyLambdaDataStore(ignored: Any) extends DataStore {
  import PhillyLambdaDataStore._

  val recordStore = new MutableRecordStore(new RecordStore(Nil))

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
    val transactionStore = initialRecordStore.copy
    var transactionLog: List[Operation] = Nil

    override def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] =
      transactionStore.retrieveRecords(condition)

    override def inTransaction[T](f: (WriteConnection) => Try[T]): Try[T] ={
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

    def commit =
      initialRecordStore.applyOperations(transactionLog.reverse)
  }
}
