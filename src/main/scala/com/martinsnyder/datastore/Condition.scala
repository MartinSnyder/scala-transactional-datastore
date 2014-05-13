package com.martinsnyder.datastore

trait Condition

case class EqualsCondition[T](fieldName: String, value: T) extends Condition