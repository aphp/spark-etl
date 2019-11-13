/*
package io.frama.parisni.spark.hive

import org.yaml.snakeyaml.Yaml
import scala.io.Source
import java.io.{ File, FileInputStream }
import org.scalatest.FunSuite

import io.frama.parisni.spark.hive.HiveToPostgresYaml._
import net.jcazevedo.moultingyaml._

class HiveToolTest extends FunSuite {

  test("test generate  update set") {
    val columns = "a" :: "b" :: Nil
    val target = "t"
    val gold = "\"a\" = \"t\".\"a\", \"b\" = \"t\".\"b\""
    val result = HIVETool.generateUpdateSet(target, columns)
    assert(gold == result)
  }

  test("test generate  update compare") {
    val columns = "a" :: "b" :: Nil
    val target = "t"
    val gold = "\"a\" <> \"t\".\"a\" OR \"b\" <> \"t\".\"b\""
    val result = HIVETool.generateUpdateCompare(target, columns)
    assert(gold == result)
  }

  test("test generate join") {
    val columns = "a" :: "b" :: Nil
    val target = "t"
    val source = "s"
    val gold = "\"t\".\"a\" = \"s\".\"a\" AND \"t\".\"b\" = \"s\".\"b\""
    val result = HIVETool.generateJoin(target, source, columns)
    assert(gold == result)
  }

  test("test generate  merge") {
    val compareColumns = "a" :: "b" :: Nil
    val updateColumns = "a" :: Nil
    val targetTable = "target"
    val key = "key1" :: "key2" :: Nil
    val columns = "col1" :: "col2" :: Nil
    val sourceTable = "source"
    val mode = "i"
    println(HIVETool.merge(targetTable, sourceTable, key, Some(columns), "i"))
    println(HIVETool.merge(targetTable, sourceTable, key, Some(columns), "iu", compareColumns = Some(compareColumns), updateColumns = Some(updateColumns)))
    println(HIVETool.merge(targetTable, sourceTable, key, Some(columns), "iud", deleteClause= Some("status=3"), compareColumns = Some(compareColumns), updateColumns = Some(updateColumns)))

  }

}


*/
