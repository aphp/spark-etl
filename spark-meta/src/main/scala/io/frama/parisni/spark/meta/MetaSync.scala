package io.frama.parisni.spark.meta

import com.typesafe.scalalogging.LazyLogging
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.SparkSession

import scala.io.Source

object MetaSync extends App with LazyLogging {

  //Log level possibilities : "ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"
  private val defaultLogLevel: String = "INFO"

  //Parsing args[] input values
  val Array(yamlPath: String, logLevel: String) = args match {

    case Array(_, _) => Array(args(0), getLogLevel(args(1)))

    case Array(_) =>
      logger.info(
        "No logLevel provided --> Default logLevel '%s' is applied".format(
          defaultLogLevel
        )
      )
      Array(args(0), defaultLogLevel)

    case _ =>
      System.err.println(
        "Expected arguments: args(0)=YamlPath\targs(1)=sparkLogLevel"
      )
      throw new UnsupportedOperationException
  }

  val spark = SparkSession
    .builder()
    .appName("cohort sync")
    .getOrCreate()

  spark.sparkContext.setLogLevel(logLevel)

  run(spark, yamlPath)

  /**
    * Get the log level to apply to the sparkContext.
    */
  private def getLogLevel(loglevel: String): String = {

    if (
      Seq("ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN")
        .contains(loglevel.trim.toUpperCase)
    ) {
      loglevel.trim.toUpperCase
    } else {
      logger.warn(
        "Loglevel %s is not recognized --> Default logLevel %s is then applied"
          .format(loglevel, defaultLogLevel)
      )
      defaultLogLevel
    }
  }

  def run(spark: SparkSession, yamlFilePath: String): Unit = {

    //Parsing input configuration Yaml file
    val ymlTxt = Source.fromFile(yamlFilePath).mkString
    val yaml = ymlTxt.stripMargin.parseYaml
    val database = yaml.convertTo[ConfigMetaYaml.Database]

    try {
      // for each datasource
      for (source <- database.schemas.getOrElse(Nil)) {
        if (source.isActive.getOrElse(true)) {
          logger.warn(s"""Extracting meta for:
               |- db name: ${source.dbName}
               |- db type: ${source.dbType}
               |- strategy:
               |    - extractor: ${source.strategy
            .getOrElse(ConfigMetaYaml.Strategy())
            .extractor
            .getOrElse(ConfigMetaYaml.ExtractStrategy())
            .featureExtractImplClass}
               |    - generator: ${source.strategy
            .getOrElse(ConfigMetaYaml.Strategy())
            .generator
            .getOrElse(ConfigMetaYaml.TableGeneratorStrategy())
            .tableGeneratorImplClass}
               |""".stripMargin)

          // get the information
          val extract = new MetaExtractor(source, spark)
          extract.initTables(source.dbName, source.schemaRegexFilter)

          // write to db
          val load = new MetaLoader(
            database.hostPg,
            database.databasePg,
            database.schemaPg,
            database.userPg,
            source.schemaRegexFilter
          )
          load.loadDatabase(extract.getDatabase, source.dbName)
          load.loadSchema(extract.getSchema, source.dbName)
          load.loadTable(extract.getTable, source.dbName)
          load.loadColumn(extract.getColumn, source.dbName)
          load.loadReference(extract.getReference, source.dbName)
        }
      }
    }
  }

}
