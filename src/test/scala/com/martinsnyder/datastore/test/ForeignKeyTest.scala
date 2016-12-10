package com.martinsnyder.datastore.test

import com.martinsnyder.datastore.memory.PhillyLambdaDataStore
import com.martinsnyder.datastore._
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

class PhillyLambdaDataStoreForeignKeyTest extends ForeignKeyTest {
  import ForeignKeyTest._

  override val dataStore = new PhillyLambdaDataStore(List(
    ForeignKeyConstraint(classOf[PointerRecord].getName, "pointer", UniqueConstraint(classOf[TargetRecord].getName, "value"))
  ))
}
