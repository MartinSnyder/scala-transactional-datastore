package com.martinsnyder.datastore.memory

import com.martinsnyder.datastore._
import scala.util.Try
import scala.reflect.ClassTag
import com.martinsnyder.datastore.DataStore.ConstraintViolation

object InMemoryDataStore {
  sealed trait TransactionOperation
  case class InsertOperation(records: Seq[Record]) extends TransactionOperation

  sealed trait ConstraintEnforcer {
    def add(records: Seq[Record]): Try[ConstraintEnforcer]
  }

  object ConstraintEnforcer {
    def apply(constraint: Constraint) = constraint match {
      case uq: UniqueConstraint => new UniqueConstraintEnforcer(uq)
    }
  }

  class UniqueConstraintEnforcer(constraint: UniqueConstraint, existingValues: Set[AnyRef] = Set()) extends ConstraintEnforcer {
    override def add(records: Seq[Record]): Try[ConstraintEnforcer] = Try({
      var values = records.map(record => record.getClass.getMethod(constraint.fieldName).invoke(record))
      var valuesSet = values.toSet

      if (valuesSet.size != values.length)
        throw ConstraintViolation(constraint)

      if (!existingValues.intersect(valuesSet).isEmpty)
        throw ConstraintViolation(constraint)

      new UniqueConstraintEnforcer(constraint, existingValues ++ valuesSet)
    })
  }

  class ConstrainedRecords(private var storedRecords: List[Record],
                           private var constraintEnforcers: List[ConstraintEnforcer]) {
    def filter(recordClass: Class[_], condition: Condition) = synchronized(Try(
      storedRecords
        .filter(record => record.getClass == recordClass)
        .filter(record => condition match {
        case EqualsCondition(fieldName, value) =>
          record.getClass.getMethod(fieldName).invoke(record) == value
      }))
    )

    private def applyConstraints(records: Seq[Record]): Try[List[ConstraintEnforcer]] = {
      val triedEnforcers = constraintEnforcers.map(_.add(records))
      triedEnforcers.find(_.isFailure) match {
        case Some(failure) =>
          failure.map(List(_))
        case None =>
          Try(triedEnforcers.map(_.get))
      }
    }

    def addRecords(records: Seq[Record]): Try[Unit] = synchronized(
      for (
        nextEnforcers <- applyConstraints(records);
        nextRecords <- Try(records.toList ::: storedRecords)
      )
      yield {
        storedRecords = nextRecords
        constraintEnforcers = nextEnforcers
      }
    )

    def applyOperations(operations: Seq[TransactionOperation]): Try[Unit] = synchronized({
      val triedOperations = operations.map({
        case InsertOperation(records) => addRecords(records)
      })

      triedOperations.find(_.isFailure) match {
        case Some(failure) =>
          failure.map(List(_))
        case None =>
          Try(())
      }
    })

    def copy = synchronized(new ConstrainedRecords(storedRecords, constraintEnforcers))
  }
}

class InMemoryDataStore(constraints: Seq[Constraint] = Nil) extends DataStore {
  import InMemoryDataStore._

  val constrainedRecords = new ConstrainedRecords(Nil, constraints.map(ConstraintEnforcer(_)).toList)

  override def withConnection[T](f: (ReadConnection) => T): T = f(new InMemoryReadConnection)

  class InMemoryReadConnection extends ReadConnection {
    override def loadRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]) =
      constrainedRecords.filter(recordTag.runtimeClass, condition)

    override def inTransaction[T](f: (WriteConnection) => Try[T]): Try[T] = {
      val writeConnection = new InMemoryWriteConnection(constrainedRecords)

      for (
        invocationResult <- f(writeConnection);
        _ <- writeConnection.commit
      ) yield invocationResult
    }
  }

  class InMemoryWriteConnection(targetContext: ConstrainedRecords) extends WriteConnection {
    val transactionContext = targetContext.copy
    var transactionOperations = List[TransactionOperation]()

    override def loadRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]) = synchronized(
      transactionContext.filter(recordTag.runtimeClass, condition))

    override def insertRecords[T <: Record](records: Seq[Record]): Try[Unit] = synchronized({
      transactionContext.addRecords(records).map(_ => {
        transactionOperations = InsertOperation(records) :: transactionOperations
        ()
      })
    })

    override def inTransaction[T](f: (WriteConnection) => Try[T]): Try[T] =
      f(new InMemoryWriteConnection(synchronized(transactionContext)))

    def commit: Try[Unit] = targetContext.applyOperations(transactionOperations.reverse)
  }
}
