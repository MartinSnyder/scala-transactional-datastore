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

import com.martinsnyder.datastore.{ AllCondition, DataStore, EqualsCondition, InMemoryDataStore, Record }
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
      // Read all records
      val retrievedAll = dataStore.withConnection(_.retrieveRecords[TestRecord](AllCondition))
      assert(retrievedAll == Success(SampleRecords))

      // Make sure that we can read the record we just inserted
      val retrievedJustFirst = dataStore.withConnection(_.retrieveRecords[TestRecord](SampleCondition))
      assert(retrievedJustFirst == Success(Seq(FirstSampleRecord)))
    }

    it("can delete records") {
      // Delete the record we inserted.  Return value should include the record
      val deleted = dataStore.withConnection(_.inTransaction(_.deleteRecords[TestRecord](SampleCondition)))
      assert(deleted == Success(Seq(FirstSampleRecord)))

      // Verify that we can no longer query for the record
      val retrieved = dataStore.withConnection(_.retrieveRecords[TestRecord](SampleCondition))
      assert(retrieved == Success(Seq()))

      // Verify that only the second record is available in the store
      val retrievedJustSecond = dataStore.withConnection(_.retrieveRecords[TestRecord](AllCondition))
      assert(retrievedJustSecond == Success(Seq(SecondSampleRecord)))
    }
  }
}

class InMemoryDataStoreBasicCRDTest extends AbstractBasicCRDTest {
  val dataStore = new InMemoryDataStore(Nil)
}
