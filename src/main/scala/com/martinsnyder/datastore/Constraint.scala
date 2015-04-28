package com.martinsnyder.datastore

// Constraint definitions
trait Constraint

// Value must be unique across all items of the same type within the data store
case class UniqueConstraint(className: String, fieldName: String) extends Constraint

// Items of this type can be added to the store but not deleted or modified
case class ImmutableConstraint(className: String) extends Constraint

// Values for sourceField must contain a value contained in the target fieldName
case class ForeignKeyConstraint(sourceClassName: String, sourceFieldName: String, target: UniqueConstraint) extends Constraint
