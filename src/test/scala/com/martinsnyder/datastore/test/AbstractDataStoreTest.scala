package com.martinsnyder.datastore.test

import org.scalatest.FunSpec
import com.martinsnyder.datastore.{EqualsCondition, DataStore, Record}
import scala.util.Success

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
  }
}
