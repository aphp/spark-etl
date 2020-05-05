package io.frama.parisni.spark.sync.copy

import com.typesafe.scalalogging.LazyLogging
import io.frama.parisni.spark.sync.Sync
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.SparkSession

import scala.io.Source
import SolrToSolrYaml._

object SolrToSolr extends App with LazyLogging {

  val filename = args(0)
  val ymlTxt = Source.fromFile(filename).mkString
  val yaml = ymlTxt.stripMargin.parseYaml
  val database = yaml.convertTo[Database]

  // Spark Session
  val spark = SparkSession.builder()
    .appName(database.jobName)
    //.enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  try {

    val dateFieldSource = database.timestampLastColumn.getOrElse("")
    val dateFieldsTarget = database.timestampColumns.getOrElse(List())
    val dateMax = database.dateMax.getOrElse("")

    for(table <- database.tables.getOrElse(Nil)) {
      //if (table.isActive.getOrElse(true)) {

        val tableSource = table.tableSolrSource.toString
        val tableTarget = table.tableSolrTarget.toString
        val zkHost = table.ZkHost.toString
        val loadType = table.typeLoad.getOrElse("full")
        val pks = table.key

        val config = Map("S_TABLE_NAME" -> tableSource, "S_TABLE_TYPE" -> "solr",
          "S_DATE_FIELD" -> dateFieldSource,

          "T_TABLE_NAME" -> tableTarget, "T_TABLE_TYPE" -> "solr",
          "ZKHOST" -> zkHost, "T_LOAD_TYPE" -> loadType, "T_DATE_MAX" -> dateMax
        )

        val sync = new Sync()
        sync.syncSourceTarget(spark, config, dateFieldsTarget, pks)

      //}
    }
  } finally {
  }
  spark.close()
}


class SolrToSolr2 extends App with LazyLogging {

  val filename = "solrToSolr.yaml"       //args(0)
  val ymlTxt = Source.fromFile(filename).mkString
  val yaml = ymlTxt.stripMargin.parseYaml
  val database = yaml.convertTo[Database]

  // Spark Session
  val spark = SparkSession.builder()
    .appName(database.jobName)
    //.enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  def sync(spark: SparkSession, database: Database, zookHost:String): Unit = {
    try {

      val dateFieldSource = database.timestampLastColumn.getOrElse("")
      val dateFieldsTarget = database.timestampColumns.getOrElse(List())
      val dateMax = database.dateMax.getOrElse("")

      for(table <- database.tables.getOrElse(Nil)) {
        //if (table.isActive.getOrElse(true)) {

        val tableSource = table.tableSolrSource.toString
        val tableTarget = table.tableSolrTarget.toString
        val zkHost = zookHost //table.ZkHost.toString
        val loadType = table.typeLoad.getOrElse("full")
        val pks = table.key

        val config = Map("S_TABLE_NAME" -> tableSource, "S_TABLE_TYPE" -> "solr",
          "S_DATE_FIELD" -> dateFieldSource,

          "T_TABLE_NAME" -> tableTarget, "T_TABLE_TYPE" -> "solr",
          "ZKHOST" -> zkHost, "T_LOAD_TYPE" -> loadType, "T_DATE_MAX" -> dateMax
        )

        val sync = new Sync()
        sync.syncSourceTarget(spark, config, dateFieldsTarget, pks)

        //}
      }

    }
  }
  spark.close()
}

