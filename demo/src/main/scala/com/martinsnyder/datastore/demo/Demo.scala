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

import com.martinsnyder.datastore.quill.Data.Person
import com.martinsnyder.datastore.quill.{ Data, DataStoreContext }
import com.martinsnyder.datastore.{ DataStore, EqualsCondition }

import scala.language.implicitConversions
import scala.util.Try

object Demo {
  def main(args: Array[String]): Unit = {

    val dataStore = Data.sampleDataStore

    def execute(description: String, f: DataStore => Try[Seq[Person]]): Unit = {
      val start = System.currentTimeMillis()
      val names: Try[Seq[String]] = f(dataStore).map(_.map(person => s"${person.givenName} ${person.familyName}"))
      val duration = System.currentTimeMillis() - start

      println(
        s"""$description
           |===========
           |$duration mSecs: $names
         """.stripMargin
      )
    }

    execute("Native query using predicate", _.withConnection(_.filter[Person](_.occupation.isEmpty)))
    execute("Native query using condition", _.withConnection(_.retrieveRecords[Person](EqualsCondition("occupation", None))))
    execute("Native query using predicate WITH index", _.withConnection(_.filter[Person](_.givenName == "Bee #91236")))
    execute("Native query using condition WITH index", _.withConnection(_.retrieveRecords[Person](EqualsCondition("givenName", "Bee #91236"))))

    execute("Quill query", dataStore => {
      val ctx = new DataStoreContext(dataStore)
      import ctx._

      //      Try(ctx.run(quote { query[Person].filter(_.occupation == None) }))
      Try(ctx.run(quote { query[Person].filter(_.givenName == "Bee #91232") }))
    })

    execute("Quill query WITH index", dataStore => {
      val ctx = new DataStoreContext(dataStore)
      import ctx._

      Try(ctx.run(quote { query[Person].filter(_.givenName == "Bee #91232") }))
    })
  }
}
