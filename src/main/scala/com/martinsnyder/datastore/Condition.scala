package com.martinsnyder.datastore

sealed trait Condition

case class EqualsCondition[T](fieldName: String, value: T) extends Condition