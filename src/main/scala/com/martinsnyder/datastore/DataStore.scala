package com.martinsnyder.datastore

trait DataStore {
  def withConnection[T](f: ReadConnection => T): T
}