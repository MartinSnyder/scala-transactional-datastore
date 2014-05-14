package com.martinsnyder.datastore.test

import com.martinsnyder.datastore.memory.InMemoryDataStore
import com.martinsnyder.datastore.UniqueConstraint

class InMemoryDataStoreTest extends AbstractDataStoreTest {
  import AbstractDataStoreTest._

  override val dataStore = new InMemoryDataStore(Seq(
    UniqueConstraint(MyRecord.getClass.getName, "value")
  ))
}
