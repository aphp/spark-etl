package io.frama.parisni.spark.hive

import io.frama.parisni.spark.hive.HiveToPostgresYaml._
import net.jcazevedo.moultingyaml._
import org.scalatest.funsuite.FunSuite

import scala.io.Source

class HiveToPostgresTest extends FunSuite {

  test("test fhir patient") {
    val filename = "hiveToPostgres.yaml"
    val ymlTxt = Source.fromFile(filename).mkString
    val yaml = ymlTxt.stripMargin.parseYaml
    val palette = yaml.convertTo[Database]

    for (pal <- palette.tables.getOrElse(Nil)) {
      println("jim" + pal.insertDatetime)
    }

    println(palette.toYaml.prettyPrint)

    assert("test_scd_pg".equals(palette.tables.get.head.tablePg))
  }
}
