package com.martinsnyder.datastore.test

import org.scalatest.FunSpec
import com.martinsnyder.datastore.{EqualsCondition, DataStore, Record}
import scala.util.{Failure, Success}
import com.martinsnyder.datastore.DataStore.ConstraintViolation

object AbstractDataStoreTest {
  case class MyRecord(value: String) extends Record
}

abstract class AbstractDataStoreTest extends FunSpec {
  import AbstractDataStoreTest._

  val dataStore: DataStore

  describe("DataStore") {
    it ("lets me insert a record") {
      val myRecord = MyRecord("testInsert")

      val insertResult = dataStore.withConnection(_.inTransaction(_.insertRecords(Seq(myRecord))))
      assert(insertResult.isSuccess)
    }

    it ("lets me retrieve a stored record") {
      val myRecord = MyRecord("testRetrieve")

      val insertResult = dataStore.withConnection(_.inTransaction(_.insertRecords(Seq(myRecord))))
      assert(insertResult.isSuccess)

      val records = dataStore.withConnection(_.loadRecords[MyRecord](EqualsCondition("value", "testRetrieve")))
      assert(records == Success(Seq(myRecord)))
    }

    it ("disallows duplicate records") {
      val myRecord = MyRecord("testDuplicate")

      val insertResult = dataStore.withConnection(_.inTransaction(_.insertRecords(Seq(myRecord))))
      assert(insertResult.isSuccess)

      dataStore.withConnection(_.inTransaction(_.insertRecords(Seq(myRecord)))) match {
        case Failure(ConstraintViolation(_)) => // Good
        case _ => fail("duplicate insert allowed")
      }
    }
  }
}
