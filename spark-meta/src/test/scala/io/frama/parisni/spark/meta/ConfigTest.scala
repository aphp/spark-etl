package io.frama.parisni.spark.meta

import ConfigMetaYaml.{Database, ExtractStrategy}
import extractor.FeatureExtractTrait
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.functions.{col, expr, lit, regexp_extract, regexp_replace, when}
import org.apache.spark.sql.{DataFrame, QueryTest}

import scala.io.Source

class ConfigTest extends QueryTest
  with SparkSessionTestWrapper {

  test("test fhir patient") {
    val filename = getClass.getResource("/meta/config.yaml").getPath
    val ymlTxt = Source.fromFile(filename).mkString
    val yaml = ymlTxt.stripMargin.parseYaml
    val palette = yaml.convertTo[Database]
    println(palette)

    for (pal <- palette.schemas.getOrElse(Nil)) {
      println(pal.dbName)
    }

    println(palette.toYaml.prettyPrint)
  }

  test("Instantiate default extract strategy from Yaml") {
    val filename = getClass.getResource("/meta/testStrategyClassConfig.yaml").getPath
    val ymlTxt = Source.fromFile(filename).mkString
    val yaml = ymlTxt.stripMargin.parseYaml
    val database = yaml.convertTo[ConfigMetaYaml.Database]
    //    println(database)
    for (schema <- database.schemas.get) {
      assert(io.frama.parisni.spark.meta.extractor.Utils.getFeatureExtractImplClass(schema.extractor).toString
      .equals("class TestFeatureExtractImpl extends FeatureExtractTrait"))
    }
  }

}

class TestFeatureExtractImpl extends FeatureExtractTrait {

  override def toString: String = "class TestFeatureExtractImpl extends FeatureExtractTrait"

  override def extractSource(df: DataFrame): DataFrame = null

  override def extractPrimaryKey(df: DataFrame): DataFrame = null

  override def extractForeignKey(df: DataFrame): DataFrame = null

  override def inferForeignKey(df: DataFrame): DataFrame = null

  override def generateDatabase(df: DataFrame): DataFrame = null

  override def generateSchema(df: DataFrame): DataFrame = null

  override def generateTable(df: DataFrame): DataFrame = null

  override def generateColumn(df: DataFrame): DataFrame = null
}