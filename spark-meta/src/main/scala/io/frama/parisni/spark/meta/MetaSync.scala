package io.frama.parisni.spark.meta

import com.typesafe.scalalogging.LazyLogging
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.SparkSession

import scala.io.Source

object MetaSync extends App with LazyLogging {

  val YAML = args(0)
  val LOG = args(1)


  val spark = SparkSession.builder()
    .appName("cohort sync")
    .getOrCreate()

  spark.sparkContext.setLogLevel(LOG)

  run(spark, YAML)

  def run(spark: SparkSession, yamlFilePath: String): Unit = {
    // for each datasource

    val ymlTxt = Source.fromFile(yamlFilePath).mkString
    val yaml = ymlTxt.stripMargin.parseYaml
    val database = yaml.convertTo[ConfigMetaYaml.Database]


    try {
      for (source <- database.schemas.getOrElse(Nil)) {
        if (source.isActive.getOrElse(true)) {
          // get the information
          val extract = new MetaExtractor(spark, source.host, source.db, source.user, source.dbType)
          extract.initTables(source.dbName)
          // write to db
          val load = new MetaLoader(database.hostPg, database.databasePg, database.schemaPg, database.userPg)
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

