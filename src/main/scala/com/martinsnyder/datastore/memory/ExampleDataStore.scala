package com.martinsnyder.datastore.memory

import com.martinsnyder.datastore.DataStore.ConstraintViolation
import com.martinsnyder.datastore._
import scala.reflect.ClassTag
import scala.util.Try
import scala.language.implicitConversions

object ExampleDataStore {
  def getFieldValue(record: Record, fieldName: String) =
    record.getClass.getMethod(fieldName).invoke(record)

  def conditionToPredicate(condition: Condition) = condition match {
    case AllCondition =>
      _: Record => true

    case EqualsCondition(fieldName, value) =>
      record: Record => getFieldValue(record, fieldName) == value

    case ExactMatchCondition(matchRecords) =>
      record: Record => matchRecords.contains(record)
  }

  class WrappedList(recordList: List[Record]) {
    def filter(condition: Condition) =
      recordList.filter(conditionToPredicate(condition))
  }
  implicit def toWrappedList(recordList: List[Record]) = new WrappedList(recordList)

  class RecordStore(private val storedRecords: List[Record], private val constraintEnforcers: List[ConstraintEnforcer]) {
    def applyConstraints(action: ConstraintEnforcer => Try[ConstraintEnforcer]): Try[List[ConstraintEnforcer]] = {
      def applyConstraints(action: ConstraintEnforcer => Try[ConstraintEnforcer], remainingCEs: List[ConstraintEnforcer]): Try[List[ConstraintEnforcer]] = {
        remainingCEs match {
          case Nil =>
            Try(Nil)

          case head :: tail =>
            for (
              nextCE <- action(head);
              tailCEs <- applyConstraints(action, tail)
            ) yield nextCE :: tailCEs
        }
      }

      applyConstraints(action, constraintEnforcers)
    }

    def createRecords[T <: Record](records: Seq[Record]): Try[RecordStore] =
      for (
        nextCEs <- applyConstraints(_.createRecords(records))
      ) yield new RecordStore(storedRecords ::: records.toList, nextCEs)

    def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] = Try({
      storedRecords
        .filter(condition)
    })

    def updateRecord[T <: Record](condition: Condition, record: Record): Try[(RecordStore, Record)] =
      for (
        oldRecord <- Try(storedRecords.filter(condition) match {
          case Nil =>
            throw new Exception("Can't find a record to update")

          case Seq(recordToUpdate) =>
            recordToUpdate

          case biggerSeq =>
            throw new Exception(s"Found too many records: ${biggerSeq.length}")
        });

        nextCEs <- applyConstraints(_.updateRecord(oldRecord, record))
      ) yield (new RecordStore(record :: storedRecords.filter(_ != oldRecord), nextCEs), oldRecord)

    def deleteRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[(RecordStore, Seq[Record])] = {
      val deleteRecords = storedRecords.filter(condition)

      for (
        nextCEs <- applyConstraints(_.deleteRecords(deleteRecords))
      ) yield (new RecordStore(storedRecords.filter(!deleteRecords.contains(_)), nextCEs), deleteRecords)
    }

    def applyOperations(transactionLog: Seq[Operation]): Try[RecordStore] = transactionLog match {
      case Nil =>
        Try(this)

      case head :: tail =>
        val nextStore = head match {
          case CreateRecordsOp(records) =>
            createRecords(records)

          case UpdateRecordOp(oldRecord, newRecord) =>
            updateRecord(ExactMatchCondition(List(oldRecord)), newRecord).map(_._1)

          case DeleteRecordsOp(deletedRecords) =>
            deleteRecords(ExactMatchCondition(deletedRecords)).map(_._1)
        }

        nextStore
          .flatMap(_.applyOperations(tail))
    }
  }

  class MutableRecordStore(var recordStore: RecordStore) {
    def createRecords[T <: Record](records: Seq[Record]): Try[Unit] =
      for (
        nextStore <- recordStore.createRecords(records)
      ) yield {
        recordStore = nextStore
      }

    def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] =
      recordStore.retrieveRecords(condition)


    def updateRecord[T <: Record](condition: Condition, record: Record): Try[Record] =
      for (
        (nextStore, updatedRecord) <- recordStore.updateRecord(condition, record)
      ) yield {
        recordStore = nextStore
        updatedRecord
      }

    def deleteRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] =
      for (
        (nextStore, deletedRecord) <- recordStore.deleteRecords(condition)
      ) yield {
        recordStore = nextStore
        deletedRecord
      }

    def copy =
      new MutableRecordStore(recordStore)

    def applyOperations(transactionLog: Seq[Operation]): Try[Unit] =
      for (
        nextStore <- recordStore.applyOperations(transactionLog)
      ) yield {
        recordStore = nextStore
      }
  }

  sealed trait Operation
  case class CreateRecordsOp(records: Seq[Record]) extends Operation
  case class UpdateRecordOp(oldRecord: Record, newRecord: Record) extends Operation
  case class DeleteRecordsOp(records: Seq[Record]) extends Operation

  object ConstraintEnforcer {
    def apply(constraint: Constraint) = constraint match {
      case uq: UniqueConstraint => new UniqueConstraintEnforcer(uq, Set())
      case iq: ImmutableConstraint => new ImmutableConstraintEnforcer(iq)
    }

  }
  sealed trait ConstraintEnforcer {
    def createRecords(records: Seq[Record]): Try[ConstraintEnforcer]
    def updateRecord(oldRecord: Record, newRecord: Record): Try[ConstraintEnforcer]
    def deleteRecords(records: Seq[Record]): Try[ConstraintEnforcer]
  }

  class UniqueConstraintEnforcer(constraint: UniqueConstraint, val uniqueValues: Set[AnyRef]) extends ConstraintEnforcer {
    override def createRecords(records: Seq[Record]): Try[ConstraintEnforcer] = Try({
      val newValues = records.map(getFieldValue(_, constraint.fieldName)).toSet

      if (newValues.size != records.size)
        throw new ConstraintViolation(constraint)

      if (newValues.intersect(uniqueValues).nonEmpty)
        throw new ConstraintViolation(constraint)

      new UniqueConstraintEnforcer(constraint, uniqueValues ++ newValues)
    })

    override def updateRecord(oldRecord: Record, newRecord: Record): Try[ConstraintEnforcer] = Try({
      val oldValue = getFieldValue(oldRecord, constraint.fieldName)
      val newValue = getFieldValue(newRecord, constraint.fieldName)

      if (oldValue == newValue)
        this
      else if (uniqueValues.contains(newValue))
        throw new ConstraintViolation(constraint)
      else
        new UniqueConstraintEnforcer(constraint, uniqueValues ++ Set(newValue))
    })

    override def deleteRecords(records: Seq[Record]): Try[ConstraintEnforcer] = Try({
      val values = records.map(getFieldValue(_, constraint.fieldName)).toSet

      new UniqueConstraintEnforcer(constraint, uniqueValues -- values)
    })
  }

  class ImmutableConstraintEnforcer(constraint: ImmutableConstraint) extends ConstraintEnforcer {
    override def createRecords(records: Seq[Record]): Try[ConstraintEnforcer] = Try(this)
    override def updateRecord(oldRecord: Record, newRecord: Record): Try[ConstraintEnforcer] = Try(throw new ConstraintViolation(constraint))
    override def deleteRecords(records: Seq[Record]): Try[ConstraintEnforcer] = Try(throw new ConstraintViolation(constraint))
  }
}

class ExampleDataStore(constraints: Seq[Constraint]) extends DataStore {
  import ExampleDataStore._

  var recordStore = new MutableRecordStore(new RecordStore(Nil, constraints.map(ConstraintEnforcer.apply).toList))

  def doTransaction[T](initialStore: MutableRecordStore, f: (WriteConnection) => Try[T]) = {
    val writeConnection = new DryRunWriteConnection(initialStore)
    for (
      result <- f(writeConnection);
      _ <- writeConnection.commit
    ) yield result
  }

  override def withConnection[T](f: (ReadConnection) => T): T =

    f(new DryRunReadConnection)


  class DryRunReadConnection extends ReadConnection {
    /**
     * Load records from the store.
     */
    override def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] =
      recordStore.retrieveRecords(condition)

    /**
     * Perform write operations in a transaction
     */
    override def inTransaction[T](f: (WriteConnection) => Try[T]): Try[T] =
      doTransaction(recordStore, f)
  }

  class DryRunWriteConnection(initialStore: MutableRecordStore) extends WriteConnection {
    val transactionContext = initialStore.copy
    var transactionLog: List[Operation] = Nil

    /**
     * Load records from the store.
     */
    override def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] =
      transactionContext.retrieveRecords(condition)

    /**
     * Perform write operations in a transaction
     */
    override def inTransaction[T](f: (WriteConnection) => Try[T]): Try[T] =
      doTransaction(transactionContext, f)

    /**
     * Add records to the store
     */
    override def createRecords[T <: Record](records: Seq[Record]): Try[Unit] =
      for (
        _ <- transactionContext.createRecords(records)
      ) yield {
        transactionLog = CreateRecordsOp(records) :: transactionLog
      }

    /**
     * Update a single existing record.  If condition does not resolve to exactly one record, then
     * an exception will be thrown.  Returns previous copy of the record
     */
    override def updateRecord[T <: Record](condition: Condition, record: Record): Try[Record] =
      for (
        oldRecord <- transactionContext.updateRecord(condition, record)
      ) yield {
        transactionLog = UpdateRecordOp(oldRecord, record) :: transactionLog
        oldRecord
      }

    /**
     * Remove records from the store that match condition.  Returns the records that were removed.
     */
    override def deleteRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] =
      for (
        deletedRecords <- transactionContext.deleteRecords(condition)
      ) yield {
        transactionLog = DeleteRecordsOp(deletedRecords) :: transactionLog
        deletedRecords
      }

    def commit: Try[Unit] =
      initialStore.applyOperations(transactionLog)
  }
}
