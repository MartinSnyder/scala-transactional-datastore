package com.martinsnyder.datastore.test

import com.martinsnyder.datastore.memory.{DataStoreGeneration2, DataStoreGeneration1}
import com.martinsnyder.datastore.{EqualsCondition, Record, DataStore}
import org.scalatest.FunSpec

import scala.util.Success

object AbstractBasicCRDTest {
  // The record type for this unit test
  case class TestRecord(value: String) extends Record

  val FirstSampleRecord = TestRecord("First Insert")
  val SecondSampleRecord = TestRecord("Second Insert")
  val SampleRecords = Seq(FirstSampleRecord, SecondSampleRecord)
  val SampleCondition = EqualsCondition("value", "First Insert")
}

/**
 * Tests basic Create, Retrieve and Delete functionality.  Updates are handled by a different test class
 *
 * This test is abstract so that it can be applied to different implementations
 */
abstract class AbstractBasicCRDTest extends FunSpec {
  import AbstractBasicCRDTest._

  // Want one data store for all of these tests.  We specifically want this to survive across
  // the different "it" clauses below.
  val dataStore: DataStore

  describe("Data Store") {
    it("can create records") {
      // There isn't much to check during insertion except that the operation succeeded.
      // The other tests depend on this working though, so we don't have to be too rigorous here
      // in terms of checking that this worked properly.
      //
      // Note that we insert two records here, but we will only be interested in one of them in the other
      // tests.  This is done to make sure that we are filtering the data store properly
      val result = dataStore.withConnection(_.inTransaction(writer => {
        writer.createRecords(SampleRecords)
      }))

      assert(result.isSuccess)
    }

    it("can retrieve records") {
      // Make sure that we can read the record we just inserted
      val retrieved = dataStore.withConnection(_.loadRecords(SampleCondition))
      assert(retrieved == Success(Seq(FirstSampleRecord)))
    }

    it("can delete records") {
      // Delete the record we inserted.  Return value should include the record
      val deleted = dataStore.withConnection(_.inTransaction(_.deleteRecords(SampleCondition)))
      assert(deleted == Success(Seq(FirstSampleRecord)))

      // Verify that we can no longer query for the record
      val retrieved = dataStore.withConnection(_.loadRecords(SampleCondition))
      assert(retrieved == Success(Seq()))
    }
  }
}

class Generation1BasicCRDTest extends AbstractBasicCRDTest {
  val dataStore = new DataStoreGeneration1
}

class Generation2BasicCRDTest extends AbstractBasicCRDTest {
  val dataStore = new DataStoreGeneration2
}
