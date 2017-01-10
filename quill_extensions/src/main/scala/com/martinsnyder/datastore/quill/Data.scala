/*
    The MIT License (MIT)

    Copyright (c) 2014 Martin Snyder

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

package com.martinsnyder.datastore.quill

import java.time.LocalDate

import com.martinsnyder.datastore.inmemory.InMemoryDataStore
import com.martinsnyder.datastore.{ DataStore, Record, UniqueConstraint }

import scala.util.Try

object Data {
  val numberWorkerBees = 1000000

  case class Person(givenName: String, familyName: String, birthday: LocalDate, occupation: Option[String]) extends Record

  private val initialPeople = Seq(
    Person("Abe", "Allen", LocalDate.of(1960, 1, 1), None),
    Person("Betsy", "Billingsly", LocalDate.of(1970, 2, 2), Some("Accountant"))
  )

  def sampleDataStore: DataStore = {
    val dataStore = new InMemoryDataStore(Seq(UniqueConstraint(classOf[Person].getName, "givenName")))

    dataStore.withConnection(_.inTransaction(conn => Try({
      conn.createRecords(initialPeople)
    })))

    // Create some worker bees
    dataStore.withConnection(_.inTransaction(conn => Try({
      conn.createRecords(
        (1 to numberWorkerBees).map(number => Person(s"Bee #$number", "Worker", LocalDate.now, Some("Busywork")))
      )
    })))

    dataStore
  }
}
