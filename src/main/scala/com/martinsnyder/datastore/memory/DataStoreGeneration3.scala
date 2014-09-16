package com.martinsnyder.datastore.memory

import com.martinsnyder.datastore.DataStore.ConstraintViolation
import com.martinsnyder.datastore._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

object DataStoreGeneration3 {
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

  class MutableRecordStore(private var recordStore: RecordStore, private var constraintEnforcers: List[ConstraintEnforcer]) {
    def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]) = synchronized(
      recordStore.retrieveRecords(condition)
    )

    def applyConstraints(op: ConstraintEnforcer => Try[ConstraintEnforcer]): Try[List[ConstraintEnforcer]] = {
      val wrappedEnforcers = constraintEnforcers.map(op)

      val failures = wrappedEnforcers.filter(_.isFailure)
      if (failures.nonEmpty) {
        failures.head.asInstanceOf[Try[List[ConstraintEnforcer]]]
      }
      else {
        Try(wrappedEnforcers.map(_.get))
      }
    }

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

    def copy =
      new MutableRecordStore(recordStore, constraintEnforcers)

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

  object ConstraintEnforcer {
    def apply(constraint: Constraint) = constraint match {
      case uq: UniqueConstraint => new UniqueConstraintEnforcer(uq)
      case iq: ImmutableConstraint => new ImmutableConstraintEnforcer(iq)
    }
  }

  trait ConstraintEnforcer {
    def addRecords(records: Seq[Record]): Try[ConstraintEnforcer]
    def updateRecord(oldRecord: Record, newRecord: Record): Try[ConstraintEnforcer]
    def deleteRecords(records: Seq[Record]): Try[ConstraintEnforcer]
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
}

class DataStoreGeneration3(val constraints: List[Constraint]) extends DataStore {
  import DataStoreGeneration3._

  private val recordStore = new MutableRecordStore(new RecordStore, constraints.map(ConstraintEnforcer.apply))

  class LocalReadConnection(baseStore: MutableRecordStore) extends ReadConnection {
    override def loadRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]) =
      recordStore.retrieveRecords(condition)

    override def inTransaction[T](f: (WriteConnection) => Try[T]): Try[T] = {
      val writeConnection = new LocalWriteConnection(recordStore)
      for (
        result <- f(writeConnection);
        _ <- writeConnection.commit()
      )
      yield {
        result
      }
    }
  }

  class LocalWriteConnection(baseStore: MutableRecordStore) extends LocalReadConnection(baseStore) with WriteConnection {
    val transactionStore = baseStore.copy
    var modifications: List[DataModification] = Nil

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

    def commit(): Try[Unit] = {
      baseStore.apply(modifications)
    }
  }

  override def withConnection[T](f: (ReadConnection) => T): T =
    f(new LocalReadConnection(recordStore))
}
