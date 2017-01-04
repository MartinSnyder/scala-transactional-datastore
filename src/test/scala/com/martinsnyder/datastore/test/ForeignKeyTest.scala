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

import com.martinsnyder.datastore._
import com.martinsnyder.datastore.inmemory.InMemoryDataStore
import org.scalatest.FunSpec

object ForeignKeyTest {
  // The record types for this unit test
  case class TargetRecord(value: String) extends Record
  case class PointerRecord(name: String, pointer: String) extends Record
}

abstract class ForeignKeyTest extends FunSpec {
  import ForeignKeyTest._

  // Want one data store for all of these tests
  val dataStore: DataStore

  describe("Data Store") {
    it("prevents insertion of records with invalid pointers") {
      val insertResult = dataStore.withConnection(_.inTransaction(_.createRecords(Seq(PointerRecord("fail", "nothing")))))
      assert(insertResult.isFailure)
    }

    it("allows insertion of records with valid pointers") {
      dataStore.withConnection(_.inTransaction(conn => {
        val res1 = conn.createRecords(Seq(TargetRecord("firstTarget")))
        assert(res1.isSuccess)

        val res2 = conn.createRecords(Seq(PointerRecord("pass", "firstTarget")))
        assert(res2.isSuccess)
        res2
      }))
    }

    it("prevents illegal deletion") {
      dataStore.withConnection(_.inTransaction(conn => {
        val res1 = conn.createRecords(Seq(TargetRecord("second"), TargetRecord("third")))
        assert(res1.isSuccess)

        val res2 = conn.createRecords(Seq(PointerRecord("pass", "second")))
        assert(res2.isSuccess)

        val res3 = conn.deleteRecords[TargetRecord](EqualsCondition("value", "second"))
        assert(res3.isFailure)

        val res4 = conn.deleteRecords[TargetRecord](EqualsCondition("value", "third"))
        assert(res4.isSuccess)

        res4
      }))
    }

  }
}

class InMemoryDataStoreForeignKeyTest extends ForeignKeyTest {
  import ForeignKeyTest._

  override val dataStore = new InMemoryDataStore(List(
    ForeignKeyConstraint(classOf[PointerRecord].getName, "pointer", UniqueConstraint(classOf[TargetRecord].getName, "value"))
  ))
}
