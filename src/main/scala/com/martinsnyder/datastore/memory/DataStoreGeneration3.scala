package com.martinsnyder.datastore.memory

import com.martinsnyder.datastore.DataStore.ConstraintViolation
import com.martinsnyder.datastore._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object DataStoreGeneration3 {
  // Uses reflection to get a named field value from a record
  def getFieldValue(record: Record, fieldName: String) =
    record.getClass.getMethod(fieldName).invoke(record)

  // Converts a condition object to a predicate that can be used to operate on Scala collections
  def conditionToPredicate(condition: Condition) = condition match {
    case AllCondition =>
      (record: Record) => true

    case EqualsCondition(fieldName, value) =>
      (record: Record) => getFieldValue(record, fieldName) == value

    case ExactMatchCondition(matchRecords) =>
      (record: Record) => matchRecords.contains(record)
  }

  // Applies a condition filter to a scala collection
  def filter(records: List[Record], condition: Condition) =
    records.filter(conditionToPredicate(condition))

  // Immutable collection that supports our basic CRUD operations
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

  // Trait for immutable stateful objects that enforce constraints
  trait ConstraintEnforcer {
    def addRecords(records: Seq[Record]): Try[ConstraintEnforcer]
    def updateRecord(oldRecord: Record, newRecord: Record): Try[ConstraintEnforcer]
    def deleteRecords(records: Seq[Record]): Try[ConstraintEnforcer]
  }

  object ConstraintEnforcer {
    def apply(constraint: Constraint) = constraint match {
      case uq: UniqueConstraint => new UniqueConstraintEnforcer(uq)
      case iq: ImmutableConstraint => new ImmutableConstraintEnforcer(iq)
    }
  }

  class UniqueConstraintEnforcer(val constraint: UniqueConstraint, uniqueValues: Set[AnyRef] = Set()) extends ConstraintEnforcer {
    override def addRecords(records: Seq[Record]): Try[ConstraintEnforcer] = Try({
      val values = records.map(getFieldValue(_, constraint.fieldName)).toSet

      // Some of our inserted records might have duplicate values
      if (values.size < records.length)
        throw new ConstraintViolation(constraint)

      if (uniqueValues.intersect(values).nonEmpty)
        throw new ConstraintViolation(constraint)

      new UniqueConstraintEnforcer(constraint, uniqueValues ++ values)
    })

    override def updateRecord(oldRecord: Record, newRecord: Record): Try[ConstraintEnforcer] = Try({
      val oldValue = getFieldValue(oldRecord, constraint.fieldName)
      val newValue = getFieldValue(newRecord, constraint.fieldName)

      if (oldValue == newValue) {
        this
      }
      else {
        if (uniqueValues.contains(newValue))
          throw new ConstraintViolation(constraint)

        new UniqueConstraintEnforcer(constraint, (uniqueValues -- Set(oldValue)) ++ Set(newValue))
      }
    })

    override def deleteRecords(records: Seq[Record]): Try[ConstraintEnforcer] = Try({
      val values = records.map(getFieldValue(_, constraint.fieldName)).toSet

      new UniqueConstraintEnforcer(constraint, uniqueValues -- values)
    })
  }

  class ImmutableConstraintEnforcer(val constraint: ImmutableConstraint) extends ConstraintEnforcer {
    override def addRecords(records: Seq[Record]): Try[ConstraintEnforcer] =
      Success(this)

    override def updateRecord(oldRecord: Record, newRecord: Record): Try[ConstraintEnforcer] =
      Failure(new ConstraintViolation(constraint))

    override def deleteRecords(records: Seq[Record]): Try[ConstraintEnforcer] =
      Failure(new ConstraintViolation(constraint))
  }

  // Mutable collection of records along with their constraint state
  class MutableRecordStore(private var recordStore: RecordStore, private var constraintEnforcers: List[ConstraintEnforcer]) {
    // Helper to apply an operation to all of the constraints.  Returns either a new collection of
    // constraints with updated state, or the first constraint violation that occurred.
    private def applyConstraints(op: ConstraintEnforcer => Try[ConstraintEnforcer]): Try[List[ConstraintEnforcer]] = {
      val wrappedEnforcers = constraintEnforcers.map(op)

      val failures = wrappedEnforcers.filter(_.isFailure)
      if (failures.nonEmpty) {
        failures.head.asInstanceOf[Try[List[ConstraintEnforcer]]]
      }
      else {
        Try(wrappedEnforcers.map(_.get))
      }
    }

    // Our basic CRUD operations
    def createRecords[T <: Record](records: Seq[Record]): Try[Unit] = synchronized(
      for (
        newStore <- recordStore.createRecords(records);
        newEnforcers <- applyConstraints(_.addRecords(records))
      )
      yield {
        recordStore = newStore
        constraintEnforcers = newEnforcers
      }
    )

    def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]) = synchronized(
      recordStore.retrieveRecords(condition)
    )

    def updateRecord[T <: Record](condition: Condition, record: Record): Try[Record] = synchronized(
      for (
        (newStore, existingRecord) <- recordStore.updateRecord(condition, record);
        newEnforcers <- applyConstraints(_.updateRecord(existingRecord, record))
      )
      yield {
        recordStore = newStore
        constraintEnforcers = newEnforcers
        existingRecord
      }
    )

    def deleteRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] = synchronized(
      for (
        (newStore, deletedRecords) <- recordStore.deleteRecords(condition);
        newEnforcers <- applyConstraints(_.deleteRecords(deletedRecords))
      )
      yield {
        recordStore = newStore
        constraintEnforcers = newEnforcers
        deletedRecords
      }
    )

    // Helper to create a copy of this mutable store (used for transactions)
    def copy =
      new MutableRecordStore(recordStore, constraintEnforcers)

    // Applies a set of modifications to this data store, atomically.
    def apply(operations: List[DataModification]): Try[Unit] = synchronized({
      // maintain our starting point, in case we encounter a failure halfway through
      val startingRecordStore = recordStore

      // Apply all of the operations to our store, collecting all of the results
      val results = operations.map({
        case CreateRecords(records) =>
          createRecords(records)

        case UpdateRecord(oldRecord, newRecord) =>
          updateRecord(ExactMatchCondition(Seq(oldRecord)), newRecord)

        case DeleteRecords(records) =>
          deleteRecords(ExactMatchCondition(records))
      })

      // Check for failures
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

class DataStoreGeneration3(val constraints: List[Constraint]) extends DataStore {
  import DataStoreGeneration3._

  private val recordStore = new MutableRecordStore(new RecordStore, constraints.map(ConstraintEnforcer.apply))

  private def doTransaction[T](baseStore: MutableRecordStore, f: (WriteConnection) => Try[T]) = {
    val writeConnection = new LocalWriteConnection(baseStore)
    for (
      result <- f(writeConnection);
      _ <- writeConnection.commit()
    )
    yield {
      result
    }
  }

  class LocalReadConnection(baseStore: MutableRecordStore) extends ReadConnection {
    override def loadRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]) =
      recordStore.retrieveRecords(condition)

    override def inTransaction[T](f: (WriteConnection) => Try[T]): Try[T] =
      doTransaction(recordStore, f)
  }

  class LocalWriteConnection(baseStore: MutableRecordStore) extends  WriteConnection {
    // Create a copy of our mutable store to be our transaction context.  We will write back
    // to the original store when commit is called
    val transactionStore = baseStore.copy

    // Track individual modifications so they can be played back during commit
    var modifications: List[DataModification] = Nil

    // Basic CRUD operations
    override def loadRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]) =
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

    // Nested transaction support
    override def inTransaction[T](f: (WriteConnection) => Try[T]): Try[T] =
      doTransaction(transactionStore, f)

    // Write our operations to the underlying base record store
    def commit(): Try[Unit] = {
      baseStore.apply(modifications)
    }
  }

  // Main entry point to the API -- exposes a read connection
  override def withConnection[T](f: (ReadConnection) => T): T =
    f(new LocalReadConnection(recordStore))
}
