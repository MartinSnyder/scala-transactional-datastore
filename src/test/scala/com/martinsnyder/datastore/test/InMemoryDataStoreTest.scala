package com.martinsnyder.datastore.test

import com.martinsnyder.datastore.memory.InMemoryDataStore

class InMemoryDataStoreTest extends AbstractDataStoreTest {
  override val dataStore = new InMemoryDataStore
}
