package com.martinsnyder.datastore.memory

import com.martinsnyder.datastore._
import scala.util.{Success, Try}
import scala.reflect.ClassTag

class InMemoryDataStore extends DataStore {
  var storedRecords = List[Record]()
  override def withConnection[T](f: (ReadConnection) => T): T = f(new InMemoryReadConnection)


  class InMemoryReadConnection extends ReadConnection {
    override def loadRecords[T <: Record](condition: Condition)(implicit recordTag: ClassTag[T]): Try[Seq[Record]] = {
      Success(
        storedRecords
          .filter(record => record.getClass == recordTag.runtimeClass)
          .filter(record => condition match {
              case EqualsCondition(fieldName, value) =>
                record.getClass.getMethod(fieldName).invoke(record) == value
            }
          )
      )
    }

    override def inTransaction[T](f: (WriteConnection) => T): Try[T] =
      Try(f(new InMemoryWriteConnection))
  }

  class InMemoryWriteConnection extends InMemoryReadConnection with WriteConnection {
    override def insertRecords[T <: Record](records: Seq[Record]): Try[Unit] = synchronized({
      storedRecords = records.toList ::: storedRecords

      Success(())
    })
  }
}
