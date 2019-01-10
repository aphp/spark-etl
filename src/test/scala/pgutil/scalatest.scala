/*
 * Copyright 2001-2009 Artima, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.aphp.eds.spark.postgres

/*
ScalaTest facilitates different styles of testing by providing traits you can mix
together to get the behavior and syntax you prefer.  A few examples are
included here.  For more information, visit:

http://www.scalatest.org/

One way to use ScalaTest is to help make JUnit or TestNG tests more
clear and concise. Here's an example:
*/
import scala.collection.mutable.Stack
import org.scalatest.Assertions
import org.junit.Test
import org.apache.spark.sql.Row


class StackSuite extends Assertions {

//  @Test def formatRowShouldBeRegularPgCsv() {
//    val l1 = (Row(null,3.2)::Row(None,2)::Nil).toSeq
//    val r1 = """,3.2
//,2"""
//    assert(PGUtil.formatRow(l1) === r1)
//
//    val l2 = (Row(null,"hello\nworld")::Row(None,"nothing")::Nil).toSeq
//    val r2 = ""","hello
//world"
//,nothing"""
//    assert(PGUtil.formatRow(l2) === r2)
//
//    val l3 = (Row(null,"hello\"\n\"world")::Row(None,"nothing")::Nil).toSeq
//    val r3 = ""","hello""
//""world"
//,nothing"""
//    assert(PGUtil.formatRow(l3) === r3)
//
//  }

}

