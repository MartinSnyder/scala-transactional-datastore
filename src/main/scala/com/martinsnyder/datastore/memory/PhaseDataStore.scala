package com.martinsnyder.datastore.memory

import com.martinsnyder.datastore.DataStore.ConstraintViolation
import com.martinsnyder.datastore._

import scala.reflect.ClassTag
import scala.util.Try

// From live-coding presentation 16-Sep-2014
// http://www.meetup.com/scala-phase/events/196933482/
object PhaseDataStore {

  def getFieldValue(record: Record, fieldName: String) =
    record.getClass.getMethod(fieldName).invoke(record)

  def conditionToPredicate(condition: Condition): Record => Boolean = condition match {
    case AllCondition =>
      _ => true

    case ExactMatchCondition(records) =>
      records.contains

    case EqualsCondition(fieldName, value) =>
      getFieldValue(_, fieldName) == value
  }

  def filter[T <: Record](records: Seq[Record], condition: Condition)(implicit recordTag: ClassTag[T]) = {
    assert(recordTag.runtimeClass != classOf[Nothing], "Filter cannot be called with Nothing")

    records
      .filter(_.getClass == recordTag.runtimeClass)
      .filter(conditionToPredicate(condition))
  }

  // Immutable
  class RecordStore(val allRecords: List[Record]) {
    def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] = Try({
      filter(allRecords, condition)
    })

    /**
     * Add records to the store
     */
    def createRecords[T <: Record](records: Seq[Record]): Try[RecordStore] = Try({
      new RecordStore(allRecords ::: records.toList)
    })

    /**
     * Update a single existing record.  If condition does not resolve to exactly one record, then
     * an exception will be thrown.  Returns previous copy of the record
     */
    def updateRecord[T <: Record](condition: Condition, record: Record): Try[(RecordStore, Record)] = Try({
      filter(allRecords, condition)(ClassTag(record.getClass)) match {
        case Seq() =>
          throw new Exception("No records")

        case Seq(foundRecord) =>
          (new RecordStore(record :: allRecords.filter(_ != foundRecord)), foundRecord)

        case biggerList =>
          throw new Exception(s"Too many records: ${biggerList.length}")
      }
    })

    /**
     * Remove records from the store that match condition.  Returns the records that were removed.
     */
    def deleteRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[(RecordStore, Seq[Record])] = Try({
      val deletedRecords = filter(allRecords, condition)

      (new RecordStore(allRecords.filter(!deletedRecords.contains(_))), deletedRecords)
    })

  }

  class MutableRecordStore(var recordStore: RecordStore, var constraintEnforcers: Seq[ConstraintEnforcer]) {
    def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] =
      recordStore.retrieveRecords(condition)

    /**
     * Add records to the store
     */
    def createRecords[T <: Record](records: Seq[Record]): Try[Unit] = synchronized(
      for (
        newRecordStore <- recordStore.createRecords(records);
        newConstraintEnforcers <- applyConstraints(_.createRecords(records))
      )
      yield {
        recordStore = newRecordStore
        constraintEnforcers = newConstraintEnforcers
      }
    )

    /**
     * Update a single existing record.  If condition does not resolve to exactly one record, then
     * an exception will be thrown.  Returns previous copy of the record
     */
    def updateRecord[T <: Record](condition: Condition, record: Record): Try[Record] = synchronized(
      for (
        (newRecordStore, oldRecord) <- recordStore.updateRecord(condition, record);
        newConstraintEnforcers <- applyConstraints(_.updateRecord(oldRecord, record))
      )
      yield {
        recordStore = newRecordStore
        constraintEnforcers = newConstraintEnforcers
        oldRecord
      }
    )

    /**
     * Remove records from the store that match condition.  Returns the records that were removed.
     */
    def deleteRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] = synchronized(
      for (
        (newRecordStore, deletedRecords) <- recordStore.deleteRecords(condition);
        newConstraintEnforcers <- applyConstraints(_.deleteRecords(deletedRecords))
      )
      yield {
        recordStore = newRecordStore
        constraintEnforcers = newConstraintEnforcers
        deletedRecords
      }
    )

    def copy =
      new MutableRecordStore(recordStore, constraintEnforcers)

    def applyConstraints(action: ConstraintEnforcer => Try[ConstraintEnforcer]): Try[Seq[ConstraintEnforcer]] = {
      val appliedResults = constraintEnforcers.map(action)

      val failures = appliedResults.filter(_.isFailure)
      if (failures.nonEmpty) {
        failures.head.asInstanceOf[Try[Seq[ConstraintEnforcer]]]
      }
      else {
        Try(appliedResults.map(_.get))
      }
    }

    def applyOperations(operations: List[TransactionOp]): Try[Unit] = {
      val initialState = recordStore

      val appliedResults = operations.map({
        case CreateOp(records) =>
          createRecords(records)

        case UpdateOp(oldRecord, newRecord) =>
          updateRecord(ExactMatchCondition(Seq(oldRecord)), newRecord)

        case DeleteOp(records) =>
          deleteRecords(ExactMatchCondition(records))(ClassTag(records.head.getClass))
      })

      val failures = appliedResults.filter(_.isFailure)
      if (failures.nonEmpty) {
        recordStore = initialState
        failures.head.asInstanceOf[Try[Unit]]
      }
      else {
        Try(())
      }
    }
  }

  sealed trait TransactionOp
  case class CreateOp(records: Seq[Record]) extends TransactionOp
  case class UpdateOp(oldRecord: Record, newRecord: Record) extends TransactionOp
  case class DeleteOp(deletedRecords: Seq[Record]) extends TransactionOp

  sealed trait ConstraintEnforcer {
    def createRecords[T <: Record](records: Seq[Record]): Try[ConstraintEnforcer]
    def updateRecord[T <: Record](oldRecord: Record, newRecord: Record): Try[ConstraintEnforcer]
    def deleteRecords[T <: Record](deletedRecords: Seq[Record])(implicit recordTag: ClassTag[T]): Try[ConstraintEnforcer]
  }

  object ConstraintEnforcer {
    def apply(constraint: Constraint) = constraint match {
      case uc: UniqueConstraint =>
        new UniqueConstraintEnforcer(uc, Set())

      case ic: ImmutableConstraint =>
        new ImmutableConstraintEnforcer(ic)
    }
  }

  class UniqueConstraintEnforcer(constraint: UniqueConstraint, uniqueValues: Set[AnyRef]) extends ConstraintEnforcer {
    override def createRecords[T <: Record](records: Seq[Record]): Try[ConstraintEnforcer] = Try({
      val newValues = records.map(getFieldValue(_, constraint.fieldName)).toSet

      if (newValues.size < records.length)
        throw new ConstraintViolation(constraint)

      if (uniqueValues.intersect(newValues).nonEmpty)
        throw ConstraintViolation(constraint)

      new UniqueConstraintEnforcer(constraint, uniqueValues ++ newValues)
    })

    override def updateRecord[T <: Record](oldRecord: Record, newRecord: Record): Try[ConstraintEnforcer] = Try({
      val oldValue = getFieldValue(oldRecord, constraint.fieldName)
      val newValue = getFieldValue(newRecord, constraint.fieldName)

      if (oldValue == newValue)
        this
      else if (uniqueValues.contains(newValue))
        throw new ConstraintViolation(constraint)
      else
        new UniqueConstraintEnforcer(constraint, (uniqueValues -- Set(oldValue)) ++ Set(newValue))
    })

    override def deleteRecords[T <: Record](deletedRecords: Seq[Record])(implicit recordTag: ClassTag[T]): Try[ConstraintEnforcer] = Try({
      val deletedValues = deletedRecords.map(getFieldValue(_, constraint.fieldName)).toSet

      new UniqueConstraintEnforcer(constraint, uniqueValues -- deletedValues)
    })
  }

  class ImmutableConstraintEnforcer(constraint: ImmutableConstraint) extends ConstraintEnforcer {
    override def createRecords[T <: Record](records: Seq[Record]): Try[ConstraintEnforcer] =
      Try(this)

    override def updateRecord[T <: Record](oldRecord: Record, newRecord: Record): Try[ConstraintEnforcer] =
      Try(throw new ConstraintViolation(constraint))

    override def deleteRecords[T <: Record](deletedRecords: Seq[Record])(implicit recordTag: ClassTag[T]): Try[ConstraintEnforcer] =
      Try(throw new ConstraintViolation(constraint))
  }
}

class PhaseDataStore(constraints: Seq[Constraint]) extends DataStore {
  import PhaseDataStore._

  var recordStore = new MutableRecordStore(new RecordStore(Nil), constraints.map(ConstraintEnforcer.apply))

  class PhaseReadConnection extends ReadConnection {
    /**
     * Load records from the store.
     */
    override def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] =
      recordStore.retrieveRecords(condition)

    /**
     * Perform write operations in a transaction
     */
    override def inTransaction[T](f: (WriteConnection) => Try[T]): Try[T] = {
      val writeConnection = new PhaseWriteConnection(recordStore)

      for (
        result <- f(writeConnection);
        _ <- writeConnection.commit()
      )
      yield {
        result
      }
    }
  }

  class PhaseWriteConnection(baseStore: MutableRecordStore) extends WriteConnection {
    val transactionStore = baseStore.copy
    var operations: List[TransactionOp] = Nil

    /**
     * Load records from the store.
     */
    override def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] =
      transactionStore.retrieveRecords(condition)

    /**
     * Add records to the store
     */
    override def createRecords[T <: Record](records: Seq[Record]): Try[Unit] =
      for (
        _ <- transactionStore.createRecords(records)
      )
      yield {
        operations = CreateOp(records) :: operations
      }

    /**
     * Update a single existing record.  If condition does not resolve to exactly one record, then
     * an exception will be thrown.  Returns previous copy of the record
     */
    override def updateRecord[T <: Record](condition: Condition, record: Record): Try[Record] =
      for (
        oldRecord <- transactionStore.updateRecord(condition, record)
      )
      yield {
        operations = UpdateOp(oldRecord, record) :: operations
        oldRecord
      }

    /**
     * Remove records from the store that match condition.  Returns the records that were removed.
     */
    override def deleteRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] =
      for (
        deletedRecords <- transactionStore.deleteRecords(condition)
      )
      yield {
        operations = DeleteOp(deletedRecords) :: operations
        deletedRecords
      }

    def commit() =
      baseStore.applyOperations(operations.reverse)

    /**
     * Perform write operations in a transaction
     */
    override def inTransaction[T](f: (WriteConnection) => Try[T]): Try[T] = {
      val writeConnection = new PhaseWriteConnection(transactionStore)

      for (
        result <- f(writeConnection);
        _ <- writeConnection.commit()
      )
      yield {
        result
      }
    }
  }

  override def withConnection[T](f: (ReadConnection) => T): T =
    f(new PhaseReadConnection)
}
