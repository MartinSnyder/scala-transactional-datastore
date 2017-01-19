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

package com.martinsnyder.datastore.demo

import com.martinsnyder.datastore.demo.Data.Person
import com.martinsnyder.datastore.quill.DataStoreContext
import com.martinsnyder.datastore.{ DataStore, EqualsCondition, Record }

import scala.language.implicitConversions
import scala.util.Try

object Demo {
  def main(args: Array[String]): Unit = {
    val dataStore = Data.sampleDataStore

    def execute(description: String, f: DataStore => Try[Seq[Record]]): Unit = {
      // Execute and throw away the result so that our timing doesn't incur any startup costs
      f(dataStore)

      // Execute again and time the result
      val start = System.currentTimeMillis()
      val names: Try[Seq[String]] = f(dataStore).map(_.map(_.toString))
      val duration = System.currentTimeMillis() - start

      // Emit the result
      println(
        s"""$description
           |===========
           |$duration mSecs: $names
         """.stripMargin
      )
    }

    execute("Predicate query", _.withConnection(_.filter[Person](_.familyName == "Allen")))
    execute("Predicate query WITH index", _.withConnection(_.filter[Person](_.id == 1)))

    execute("Condition query", _.withConnection(_.retrieveRecords[Person](EqualsCondition("familyName", "Allen"))))
    execute("Condition query WITH index", _.withConnection(_.retrieveRecords[Person](EqualsCondition("id", 1))))

    execute("Quill query", dataStore => {
      val ctx = new DataStoreContext(dataStore, List(classOf[Person]))
      import ctx._

      val familyNameToFind = "Allen"

      Try(ctx.run(quote { query[Person].filter(_.familyName == "Allen") }))
    })

    execute("Quill query WITH index", ds => Try(getPersonById(ds, 1).toList))

    def getPersonById(dataStore: DataStore, id: Int): Option[Person] = {
      val ctx = new DataStoreContext(dataStore, List(classOf[Person]))
      import ctx._

      ctx.run(quote { query[Person].filter(_.id == lift(id)) }).headOption
    }
  }
}
