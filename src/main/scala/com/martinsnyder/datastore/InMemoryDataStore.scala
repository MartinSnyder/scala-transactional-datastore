package com.martinsnyder.datastore

import com.martinsnyder.datastore.DataStore.ConstraintViolation

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.util.{ Success, Try }

// Implementation from presentation preparation.
// This was the first implementation to implement foreign keys
object InMemoryDataStore {
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

  class RecordStore(storedRecords: List[Record], enforcers: List[ConstraintEnforcer]) {
    /**
     * Add records to the store
     */
    def createRecords[T <: Record](records: Seq[Record]): Try[RecordStore] =
      for (
        newEnforcers <- applyConstraints(_.createRecords(records))
      ) yield new RecordStore(storedRecords ::: records.toList, newEnforcers)

    /**
     * Load records from the store.
     */
    def retrieveRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] =
      queryConstraints[T](condition).map({
        case None =>
          storedRecords.filter(condition)

        case Some(results) =>
          results
      })

    /**
     * Update a single existing record.  If condition does not resolve to exactly one record, then
     * an exception will be thrown.  Returns previous copy of the record
     */
    def updateRecord[T <: Record](condition: Condition, record: Record): Try[(RecordStore, Record)] =
      for (
        (newRecords, oldRecord) <- Try(
          storedRecords.filter(condition) match {
            case Nil =>
              throw new Exception("Could not find record)")

            case Seq(recordToUpdate) =>
              (record :: storedRecords.filter(_ != recordToUpdate), recordToUpdate)

            case biggerSequence =>
              throw new Exception(s"Too many records ${biggerSequence.length}")
          }
        );

        newEnforcers <- applyConstraints(_.updateRecord(oldRecord, record))
      ) yield (new RecordStore(newRecords, newEnforcers), oldRecord)

    /**
     * Remove records from the store that match condition.  Returns the records that were removed.
     */
    def deleteRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[(RecordStore, Seq[Record])] =
      for (
        (applicableRecords, otherRecords) <- Try(storedRecords.partition(_.getClass == recordTag.runtimeClass));
        (recordsToDelete, recordsToKeep) <- Try(applicableRecords.partition(condition));

        newEnforcers <- applyConstraints(_.deleteRecords(recordsToDelete))
      ) yield (new RecordStore(otherRecords ::: recordsToKeep, newEnforcers), recordsToDelete)

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
              store.deleteRecords(ExactMatchCondition(deletedRecords))
                .map(_._1)
          }
        })
      }

      transactionLog.foldLeft(Try(this))(privateCombine)
    }

    private def applyConstraints(action: ConstraintEnforcer => Try[ConstraintEnforcer]): Try[List[ConstraintEnforcer]] = {
      def privateApply(maybeProgress: Try[List[ConstraintEnforcer]], next: ConstraintEnforcer): Try[List[ConstraintEnforcer]] = {
        maybeProgress.flatMap(progress => {
          val stuff = action(next).map(_ :: progress)
          stuff
        })
      }

      val result = enforcers.foldLeft(Try(List[ConstraintEnforcer]()))(privateApply)
      result
    }

    private def queryConstraints[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Option[Seq[T]]] = {
      def privateApply(maybeProgress: Try[Option[Seq[T]]], next: ConstraintEnforcer): Try[Option[Seq[T]]] =
        maybeProgress.flatMap({
          case None =>
            next.retrieve(recordTag.runtimeClass.getName, condition).map(_.map(_.map(_.asInstanceOf[T])))

          case results =>
            Try(results)
        })

      enforcers.foldLeft(Try(None: Option[Seq[T]]))(privateApply)
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

  object ConstraintEnforcer {
    def apply(constraint: Constraint): ConstraintEnforcer = constraint match {
      case uc: UniqueConstraint => new UniqueConstraintEnforcer(uc, Map())
      case ic: ImmutableConstraint => new ImmutableConstraintEnforcer(ic)
      case fkc: ForeignKeyConstraint => new ForeignKeyConstraintEnforcer(fkc, Map(), ConstraintEnforcer(fkc.target))
    }
  }
  sealed trait ConstraintEnforcer {
    def createRecords[T <: Record](records: Seq[Record]): Try[ConstraintEnforcer]
    def updateRecord[T <: Record](oldRecord: Record, newRecord: Record): Try[ConstraintEnforcer]
    def deleteRecords[T <: Record](records: Seq[Record]): Try[ConstraintEnforcer]
    def retrieve(className: String, condition: Condition): Try[Option[Seq[Record]]] = Try(None)
  }

  class ForeignKeyConstraintEnforcer(constraint: ForeignKeyConstraint, currentValues: Map[AnyRef, Int], targetValues: ConstraintEnforcer) extends ConstraintEnforcer {
    override def createRecords[T <: Record](records: Seq[Record]) = {
      val applicableRecords = records.filter(_.getClass.getName == constraint.sourceClassName)
      val incomingValues = applicableRecords.map(getFieldValue(_, constraint.sourceFieldName))

      // Verify that all these incoming values are legit
      val targetRecords = incomingValues.map(value =>
        targetValues.retrieve(constraint.target.className, EqualsCondition(constraint.target.fieldName, value)) match {
          case Success(Some(Seq(record))) =>
            Some(record)

          case _ =>
            None
        })

      // Pointer to a non-existent target
      if (targetRecords.filter(_.isEmpty).nonEmpty) {
        Try(throw new ConstraintViolation(constraint))
      } else {
        val updatedValues = incomingValues.foldLeft(currentValues)((progress, nextVal) =>
          progress.get(nextVal) match {
            case Some(count) =>
              progress + (nextVal -> (count + 1))
            case None =>
              progress + (nextVal -> 1)
          })

        for (
          updatedTargetValues <- targetValues.createRecords(records)
        ) yield new ForeignKeyConstraintEnforcer(constraint, updatedValues, updatedTargetValues)
      }
    }

    override def updateRecord[T <: Record](oldRecord: Record, newRecord: Record) = ???

    override def deleteRecords[T <: Record](records: Seq[Record]) =
      records.headOption.map(_.getClass.getName) match {
        case None =>
          Try(this)

        case Some(className) =>
          if (className == constraint.target.className) {
            val values = records.map(getFieldValue(_, constraint.target.fieldName))
            val occurrences = values.map(currentValues.getOrElse(_, 0: Int)).sum

            if (occurrences > 0) {
              Try(throw new ConstraintViolation(constraint))
            } else {
              for (
                updatedTargetValues <- targetValues.deleteRecords(records)
              ) yield new ForeignKeyConstraintEnforcer(constraint, currentValues, updatedTargetValues)
            }
          } else {
            Try(this)
          }
      }
  }

  class UniqueConstraintEnforcer(constraint: UniqueConstraint, uniqueValues: Map[AnyRef, Record]) extends ConstraintEnforcer {
    override def createRecords[T <: Record](records: Seq[Record]) = Try({
      val applicableRecords = records.filter(_.getClass.getName == constraint.className)
      val incomingValues = applicableRecords.map(record => getFieldValue(record, constraint.fieldName) -> record)

      val updatedValues = uniqueValues ++ incomingValues.toMap
      if (updatedValues.size != (uniqueValues.size + applicableRecords.size))
        throw ConstraintViolation(constraint)

      new UniqueConstraintEnforcer(constraint, updatedValues)
    })

    override def updateRecord[T <: Record](oldRecord: Record, newRecord: Record) = Try(
      if (oldRecord.getClass.getName == newRecord.getClass.getName && newRecord.getClass.getName == constraint.className) {
        val oldValue = getFieldValue(oldRecord, constraint.fieldName)
        val newValue = getFieldValue(newRecord, constraint.fieldName)

        val updatedValues = uniqueValues - oldValue + (newValue -> newRecord)
        if (uniqueValues.size != updatedValues.size)
          throw ConstraintViolation(constraint)

        new UniqueConstraintEnforcer(constraint, updatedValues)

        this
      } else {
        this
      }
    )

    override def deleteRecords[T <: Record](records: Seq[Record]) = Try({
      val applicableRecords = records.filter(_.getClass.getName == constraint.className)
      val outgoingValues = applicableRecords.map(getFieldValue(_, constraint.fieldName))

      val updatedValues = uniqueValues -- outgoingValues
      if (updatedValues.size != (uniqueValues.size - applicableRecords.size))
        throw ConstraintViolation(constraint)

      new UniqueConstraintEnforcer(constraint, updatedValues)
    })

    override def retrieve(className: String, condition: Condition): Try[Option[Seq[Record]]] = Try(
      condition match {
        case EqualsCondition(conditionField, conditionValue: AnyRef) =>
          if (className == constraint.className && conditionField == constraint.fieldName) {
            uniqueValues.get(conditionValue).map(record => List(record))
          } else {
            None
          }

        case _ =>
          None
      }
    )
  }

  class ImmutableConstraintEnforcer(constraint: ImmutableConstraint) extends ConstraintEnforcer {
    override def createRecords[T <: Record](records: Seq[Record]) =
      Try(this)
    override def updateRecord[T <: Record](oldRecord: Record, newRecord: Record) =
      Try(throw ConstraintViolation(constraint))
    override def deleteRecords[T <: Record](records: Seq[Record]) =
      Try(throw ConstraintViolation(constraint))
  }
}

class InMemoryDataStore(constraints: Seq[Constraint]) extends DataStore {
  import InMemoryDataStore._

  val recordStore = new MutableRecordStore(new RecordStore(Nil, constraints.map(ConstraintEnforcer.apply).toList))

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

    override def inTransaction[T](f: (WriteConnection) => Try[T]): Try[T] = {
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
