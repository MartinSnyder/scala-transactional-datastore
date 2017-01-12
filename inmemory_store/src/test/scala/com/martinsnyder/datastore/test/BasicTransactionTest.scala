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

import com.martinsnyder.datastore.inmemory.InMemoryDataStore
import com.martinsnyder.datastore.{ AllCondition, DataStore, Record }
import org.scalatest.FunSpec

import scala.util.Success

object AbstractBasicTransactionTest {
  // The record type for this unit test
  case class TestRecord(value: String) extends Record

  val FirstSampleRecord = TestRecord("First Insert")
  val SecondSampleRecord = TestRecord("Second Insert")
}

abstract class AbstractBasicTransactionTest extends FunSpec {
  import AbstractBasicTransactionTest._

  // Want one data store for all of these tests.  We specifically want this to survive across
  // the different "it" clauses below.
  val dataStore: DataStore

  describe("DataStore Transactions") {
    they("are isolated from each other") {
      dataStore.withConnection(readConnection => {
        readConnection.inTransaction(txn1 => {
          // Verify there are no matching records
          assert(txn1.retrieveRecords[TestRecord](AllCondition()) == Success(Seq()))

          // Insert a record in the outer transaction
          txn1.createRecords(Seq(FirstSampleRecord))

          // Verify that this is the only record available
          assert(txn1.retrieveRecords[TestRecord](AllCondition()) == Success(Seq(FirstSampleRecord)))

          readConnection.inTransaction(txn2 => {
            // Verify there are no matching records
            assert(txn2.retrieveRecords[TestRecord](AllCondition()) == Success(Seq()))

            // Insert a record in the outer transaction
            txn2.createRecords(Seq(SecondSampleRecord))

            // Verify that this is the only record available
            assert(txn2.retrieveRecords[TestRecord](AllCondition()) == Success(Seq(SecondSampleRecord)))

            Success(())
          })

          // The second transaction has committed, but we haven't.  We should still only see one record
          assert(txn1.retrieveRecords[TestRecord](AllCondition()) == Success(Seq(FirstSampleRecord)))

          Success(())
        })
      })

      // Both transactions have committed, both records should be visible here.  There is no guarantee
      // about the order, but we have fortunately chosen the correct order for our comparison :)
      dataStore.withConnection(readConnection => {
        assert(readConnection.retrieveRecords[TestRecord](AllCondition()) == Success(Seq(SecondSampleRecord, FirstSampleRecord)))
      })
    }
  }
}

class InMemoryDataStoreBasicTransactionTest extends AbstractBasicTransactionTest {
  val dataStore = new InMemoryDataStore(Nil)
}
