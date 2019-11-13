package io.frama.parisni.spark.hive

import org.yaml.snakeyaml.Yaml
import scala.io.Source
import java.io.{ File, FileInputStream }

import io.frama.parisni.spark.hive.PostgresToHiveYaml._
import net.jcazevedo.moultingyaml._
import org.scalatest.FunSuite

class PostgresToHiveTest extends FunSuite {

  test("test fhir patient") {
    val filename = "postgresToHive.yaml"
    val ymlTxt = Source.fromFile(filename).mkString
    val yaml = ymlTxt.stripMargin.parseYaml
    val palette = yaml.convertTo[Database]
    
    
    for (pal <- palette.tables.getOrElse(Nil)) {
      println("bob" + pal.isActive.get.toString())
    }
   println(palette.toYaml.prettyPrint)

  }

}


