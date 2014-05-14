package com.martinsnyder.datastore

object DataStore {
  case class ConstraintViolation(constraint: Constraint) extends Exception(s"Constraint Violated: $constraint")
}

trait DataStore {
  def withConnection[T](f: ReadConnection => T): T
}