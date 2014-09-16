package com.martinsnyder.datastore

// Condition objects for querying the store
sealed trait Condition

// Matches all records
case object AllCondition extends Condition

// Matches all records where the record's value for the specified field matches
case class EqualsCondition[T](fieldName: String, value: T) extends Condition

// Returns exactly the record that are provided in the query.  Can be used to test
// for existence
case class ExactMatchCondition(records: Seq[Record]) extends Condition