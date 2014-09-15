package com.martinsnyder.datastore

sealed trait Condition

case object AllCondition extends Condition
case class EqualsCondition[T](fieldName: String, value: T) extends Condition
case class ExactMatchCondition(records: Seq[Record]) extends Condition