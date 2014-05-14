package com.martinsnyder.datastore

sealed trait Constraint

case class UniqueConstraint(className: String, fieldName: String) extends Constraint