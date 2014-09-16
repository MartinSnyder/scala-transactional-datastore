package com.martinsnyder.datastore

object DataStore {
  case class ConstraintViolation(constraint: Constraint) extends Exception(s"Constraint Violated: $constraint")
}

// Data stores have a very simple connection-based interface
trait DataStore {
  def withConnection[T](f: ReadConnection => T): T
}