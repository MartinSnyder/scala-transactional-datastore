/*
    The MIT License (MIT)

    Copyright (c) 2014 Martin Snyder

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

package com.martinsnyder.datastore.test

import org.scalatest.FunSpec
import com.martinsnyder.datastore.{ DataStore, EqualsCondition, Record, UniqueConstraint }

import scala.util.{ Failure, Success, Try }
import com.martinsnyder.datastore.DataStore.ConstraintViolation
import com.martinsnyder.datastore.inmemory.InMemoryDataStore

object ConstraintAndTransactionTest {
  case class MyRecord(value: String) extends Record
}

abstract class ConstraintAndTransactionTest extends FunSpec {
  import ConstraintAndTransactionTest._

  val dataStore: DataStore

  describe("DataStore") {
    it("lets me insert a record") {
      val myRecord = MyRecord("testInsert")

      val insertResult = dataStore.withConnection(_.inTransaction(_.createRecords(Seq(myRecord))))
      assert(insertResult.isSuccess)
    }

    it("lets me retrieve a stored record") {
      val myRecord = MyRecord("testRetrieve")

      val insertResult = dataStore.withConnection(_.inTransaction(_.createRecords(Seq(myRecord))))
      assert(insertResult.isSuccess)

      val records = dataStore.withConnection(_.retrieveRecords[MyRecord](EqualsCondition("value", "testRetrieve")))
      assert(records == Success(Seq(myRecord)))
    }

    it("disallows duplicate records") {
      val myRecord = MyRecord("testDuplicate")

      val insertResult = dataStore.withConnection(_.inTransaction(_.createRecords(Seq(myRecord))))
      assert(insertResult.isSuccess)

      dataStore.withConnection(_.inTransaction(_.createRecords(Seq(myRecord)))) match {
        case Failure(ConstraintViolation(_)) => // Good
        case _ => fail("duplicate insert allowed")
      }
    }

    it("supports transaction rollback") {
      val myRecord = MyRecord("testTransactionRollback")

      val transactionResult = dataStore.withConnection(_.inTransaction(connection => Try({
        val insertResult = connection.createRecords(Seq(myRecord))
        assert(insertResult.isSuccess)

        val records = connection.retrieveRecords[MyRecord](EqualsCondition("value", "testTransactionRollback"))
        assert(records == Success(Seq(myRecord)))

        throw new Exception("whoops!")
      })))

      assert(transactionResult.isFailure)

      val records = dataStore.withConnection(_.retrieveRecords[MyRecord](EqualsCondition("value", "testTransactionRollback")))
      assert(records == Success(Seq()))
    }

    it("rejects conflicting transactions") {
      val myRecord = MyRecord("testTransactionConflict")

      dataStore.withConnection(connection => {
        val transaction1Result = connection.inTransaction(writeConnection1 => Try({
          val insertResult1 = writeConnection1.createRecords(Seq(myRecord))
          assert(insertResult1.isSuccess)

          val transaction2Result = connection.inTransaction(writeConnection2 => Try({
            val insertResult2 = writeConnection2.createRecords(Seq(myRecord))
            assert(insertResult2.isSuccess)
          }))

          assert(transaction2Result.isSuccess)
        }))

        assert(transaction1Result.isFailure)
      })
    }

    it("lets me delete a stored record") {
      val myRecord = MyRecord("testDelete")

      val insertResult = dataStore.withConnection(_.inTransaction(_.createRecords(Seq(myRecord))))
      assert(insertResult.isSuccess)

      // Should NOT be permitted to insert it again
      val duplicateInsert1Result = dataStore.withConnection(_.inTransaction(_.createRecords(Seq(myRecord))))
      assert(duplicateInsert1Result.isFailure)

      val records = dataStore.withConnection(_.retrieveRecords[MyRecord](EqualsCondition("value", "testDelete")))
      assert(records == Success(Seq(myRecord)))

      val deleteResult = dataStore.withConnection(_.inTransaction(_.deleteRecords[MyRecord](EqualsCondition("value", "testDelete"))))
      assert(deleteResult == Success(Seq(MyRecord("testDelete"))))

      // Should be permitted to insert it again
      val duplicateInsert2Result = dataStore.withConnection(_.inTransaction(_.createRecords(Seq(myRecord))))
      assert(duplicateInsert2Result.isSuccess)
    }
  }
}

class InMemoryDataStoreConstraintAndTransactionTest extends ConstraintAndTransactionTest {
  import ConstraintAndTransactionTest._

  override val dataStore = new InMemoryDataStore(List(
    UniqueConstraint(classOf[MyRecord].getName, "value")
  ))
}
