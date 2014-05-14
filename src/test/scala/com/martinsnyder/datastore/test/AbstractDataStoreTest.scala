package com.martinsnyder.datastore.test

import org.scalatest.FunSpec
import com.martinsnyder.datastore.{EqualsCondition, DataStore, Record}
import scala.util.{Try, Failure, Success}
import com.martinsnyder.datastore.DataStore.ConstraintViolation

object AbstractDataStoreTest {
  case class MyRecord(value: String) extends Record
}

abstract class AbstractDataStoreTest extends FunSpec {
  import AbstractDataStoreTest._

  val dataStore: DataStore

  describe("DataStore") {
    it("lets me insert a record") {
      val myRecord = MyRecord("testInsert")

      val insertResult = dataStore.withConnection(_.inTransaction(_.insertRecords(Seq(myRecord))))
      assert(insertResult.isSuccess)
    }

    it("lets me retrieve a stored record") {
      val myRecord = MyRecord("testRetrieve")

      val insertResult = dataStore.withConnection(_.inTransaction(_.insertRecords(Seq(myRecord))))
      assert(insertResult.isSuccess)

      val records = dataStore.withConnection(_.loadRecords[MyRecord](EqualsCondition("value", "testRetrieve")))
      assert(records == Success(Seq(myRecord)))
    }

    it("disallows duplicate records") {
      val myRecord = MyRecord("testDuplicate")

      val insertResult = dataStore.withConnection(_.inTransaction(_.insertRecords(Seq(myRecord))))
      assert(insertResult.isSuccess)

      dataStore.withConnection(_.inTransaction(_.insertRecords(Seq(myRecord)))) match {
        case Failure(ConstraintViolation(_)) => // Good
        case _ => fail("duplicate insert allowed")
      }
    }

    it("supports transaction rollback") {
      val myRecord = MyRecord("testTransactionRollback")

      val transactionResult = dataStore.withConnection(_.inTransaction(connection => Try({
        val insertResult = connection.insertRecords(Seq(myRecord))
        assert(insertResult.isSuccess)

        val records = connection.loadRecords[MyRecord](EqualsCondition("value", "testTransactionRollback"))
        assert(records == Success(Seq(myRecord)))

        throw new Exception("whoops!")
      })))

      assert(transactionResult.isFailure)

      val records = dataStore.withConnection(_.loadRecords[MyRecord](EqualsCondition("value", "testTransactionRollback")))
      assert(records == Success(Seq()))
    }

    it("rejects conflicting transactions") {
      val myRecord = MyRecord("testTransactionConflict")

      dataStore.withConnection(connection => {
        val transaction1Result = connection.inTransaction(writeConnection1 => Try({
          val insertResult1 = writeConnection1.insertRecords(Seq(myRecord))
          assert(insertResult1.isSuccess)


          val transaction2Result = connection.inTransaction(writeConnection2 => Try({
            val insertResult2 = writeConnection2.insertRecords(Seq(myRecord))
            assert(insertResult2.isSuccess)
          }))

          assert(transaction2Result.isSuccess)
        }))

        assert(transaction1Result.isFailure)
      })
    }
  }
}
