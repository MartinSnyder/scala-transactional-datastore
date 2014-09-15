package com.martinsnyder.datastore.test

import com.martinsnyder.datastore.memory.DataStoreGeneration2
import com.martinsnyder.datastore.{AllCondition, DataStore, Record}
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
          assert(txn1.loadRecords(AllCondition) == Success(Seq()))

          // Insert a record in the outer transaction
          txn1.createRecords(Seq(FirstSampleRecord))

          // Verify that this is the only record available
          assert(txn1.loadRecords(AllCondition) == Success(Seq(FirstSampleRecord)))

          readConnection.inTransaction(txn2 => {
            // Verify there are no matching records
            assert(txn2.loadRecords(AllCondition) == Success(Seq()))

            // Insert a record in the outer transaction
            txn2.createRecords(Seq(SecondSampleRecord))

            // Verify that this is the only record available
            assert(txn2.loadRecords(AllCondition) == Success(Seq(SecondSampleRecord)))

            Success()
          })

          // The second transaction has committed, but we haven't.  We should still only see one record
          assert(txn1.loadRecords(AllCondition) == Success(Seq(FirstSampleRecord)))

          Success()
        })
      })

      // Both transactions have committed, both records should be visible here.  There is no guarantee
      // about the order, but we have fortunately chosen the correct order for our comparison :)
      dataStore.withConnection(readConnection => {
        assert(readConnection.loadRecords(AllCondition) == Success(Seq(SecondSampleRecord, FirstSampleRecord)))
      })
    }
  }
}


class Generation2BasicTransactionTest extends AbstractBasicTransactionTest {
  val dataStore = new DataStoreGeneration2
}
