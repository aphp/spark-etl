package io.frama.parisni.spark.meta

import ConfigMetaYaml.Database
import io.frama.parisni.spark.meta.strategy.MetaStrategyBuilder
import io.frama.parisni.spark.meta.strategy.extractor.FeatureExtractTrait
import io.frama.parisni.spark.meta.strategy.generator.TableGeneratorTrait
import net.jcazevedo.moultingyaml._
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

  test("Build MetaStrategy from YAML") {
    //Parse yaml file into ConfigMetaYaml.Database
    val filename = getClass.getResource("/meta/test-strategy.yaml").getPath
    val ymlTxt = Source.fromFile(filename).mkString
    val yaml = ymlTxt.stripMargin.parseYaml
    val database = yaml.convertTo[ConfigMetaYaml.Database]

    /**
      * First test with test strategy class
      */
    var schema: ConfigMetaYaml.Schema = database.schemas.getOrElse(null).apply(0)
    assert(MetaStrategyBuilder.build(schema.strategy).extractor.toString
      .equals(new TestFeatureExtractImpl().toString))

    assert(MetaStrategyBuilder.build(schema.strategy).generator.toString
      .equals(new TestTableGeneratorImpl().toString))

    /**
      * Second test with empty strategy class -- we expect defaultStrategy
      */
    schema = database.schemas.getOrElse(null).apply(1)
    assert(MetaStrategyBuilder.build(schema.strategy).extractor.toString
      .equals(MetaStrategyBuilder.build().extractor.toString))

    assert(MetaStrategyBuilder.build(schema.strategy).generator.toString
      .equals(MetaStrategyBuilder.build().generator.toString))

    /**
      * Third test with test extractor strategy class only -- we expect default generator strategy class
      */
    schema = database.schemas.getOrElse(null).apply(2)
    assert(MetaStrategyBuilder.build(schema.strategy).extractor.toString
      .equals(new TestFeatureExtractImpl().toString))

    assert(MetaStrategyBuilder.build(schema.strategy).generator.toString
      .equals(MetaStrategyBuilder.build().generator.toString))

    /**
      * Fourth test with test generator strategy class only -- we expect default extractor strategy class
      */
    schema = database.schemas.getOrElse(null).apply(3)
    assert(MetaStrategyBuilder.build(schema.strategy).extractor.toString
      .equals(MetaStrategyBuilder.build().extractor.toString))

    assert(MetaStrategyBuilder.build(schema.strategy).generator.toString
      .equals(new TestTableGeneratorImpl().toString))
  }

}

class TestFeatureExtractImpl extends FeatureExtractTrait {

  override def toString: String = "class TestFeatureExtractImpl extends FeatureExtractTrait"

  override def extractSource(df: DataFrame): DataFrame = null

  override def extractPrimaryKey(df: DataFrame): DataFrame = null

  override def extractForeignKey(df: DataFrame): DataFrame = null

  override def inferForeignKey(df: DataFrame): DataFrame = null
}

class TestTableGeneratorImpl extends TableGeneratorTrait {

  override def toString: String = "class TestTableGeneratorImpl extends TableGeneratorTrait"

  override def generateDatabase(df: DataFrame): DataFrame = null

  override def generateSchema(df: DataFrame): DataFrame = null

  override def generateTable(df: DataFrame): DataFrame = null

  override def generateColumn(df: DataFrame): DataFrame = null
}
