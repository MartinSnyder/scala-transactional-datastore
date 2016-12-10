package com.martinsnyder.datastore.test

import com.martinsnyder.datastore.{ DataStore, EqualsCondition, InMemoryDataStore, Record }
import org.scalatest.FunSpec

import scala.util.Success

object AbstractBasicUpdateTest {
  // The record type for this unit test
  case class TestRecord(value: String) extends Record

  val InsertSampleRecord = TestRecord("First Insert")
  val UpdateSampleRecord = TestRecord("First Update")
  val SampleCondition = EqualsCondition("value", "First Insert")
}

abstract class AbstractBasicUpdateTest extends FunSpec {
  import AbstractBasicUpdateTest._

  // Want one data store for all of these tests
  val dataStore: DataStore

  describe("Data Store") {
    it("throws an exception if it can't find a record to update") {
      // The data store will be empty on initialization, so no matter what we pass to update
      // record here, it will fail.
      val result = dataStore.withConnection(_.inTransaction(_.updateRecord(SampleCondition, UpdateSampleRecord)))
      assert(result.isFailure)
    }

    it("can update records") {
      // Get started with one record
      dataStore.withConnection(_.inTransaction(_.createRecords(Seq(InsertSampleRecord))))

      // Update the record with new values.  The existing record should be returned
      val updated = dataStore.withConnection(_.inTransaction(_.updateRecord(SampleCondition, UpdateSampleRecord)))
      assert(updated == Success(InsertSampleRecord))

      // Attempt to query for the original record.  It should no longer be available
      val retrievedOriginal = dataStore.withConnection(_.retrieveRecords[TestRecord](SampleCondition))
      assert(retrievedOriginal == Success(Seq()))

      // Query for the updated values.  They should exist in the store
      val retrievedUpdate = dataStore.withConnection(_.retrieveRecords[TestRecord](EqualsCondition("value", "First Update")))
      assert(retrievedUpdate == Success(Seq(UpdateSampleRecord)))
    }

    it("throws an exception when it finds too many records to update") {
      dataStore.withConnection(_.inTransaction(_.createRecords(Seq(InsertSampleRecord, InsertSampleRecord))))

      val result = dataStore.withConnection(_.inTransaction(_.updateRecord(SampleCondition, UpdateSampleRecord)))
      assert(result.isFailure)
    }
  }
}

class InMemoryDataStoreBasicUpdateTest extends AbstractBasicUpdateTest {
  val dataStore = new InMemoryDataStore(Nil)
}
