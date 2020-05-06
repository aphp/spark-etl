package io.frama.parisni.spark.sync.copy

import com.typesafe.scalalogging.LazyLogging
import io.frama.parisni.spark.sync.Sync
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.SparkSession

import scala.io.Source
import DeltaToSolrYaml._

object SolrToDelta extends App with LazyLogging {

  val filename = args(0)
  val ymlTxt = Source.fromFile(filename).mkString
  val yaml = ymlTxt.stripMargin.parseYaml
  val database = yaml.convertTo[Database]

  // Spark Session
  val spark = SparkSession.builder()
    .appName(database.jobName)
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  try {

    val dateFieldSolr = database.timestampLastColumn.getOrElse("")
    val dateFieldsDelta = database.timestampColumns.getOrElse(List())
    val dateMax = database.dateMax.getOrElse("")

    for(table <- database.tables.getOrElse(Nil)) {
      //if (table.isActive.getOrElse(true)) {

        val tableDelta = table.tableDelta.toString
        val pathDelta = table.schemaDelta.toString
        val tableSolr = table.tableSolr.toString
        val zkHost = table.ZkHost.toString
        val loadType = table.typeLoad.getOrElse("full")
        val pks = table.key

        val config = Map("S_TABLE_NAME" -> tableSolr, "S_TABLE_TYPE" -> "solr",
          "S_DATE_FIELD" -> dateFieldSolr, "ZKHOST" -> zkHost,

          "T_TABLE_NAME" -> tableDelta, "T_TABLE_TYPE" -> "delta",
          "PATH" -> pathDelta, "T_LOAD_TYPE" -> loadType, "T_DATE_MAX" -> dateMax
        )

        val sync = new Sync()
        sync.syncSourceTarget(spark, config, dateFieldsDelta, pks)

      //}
    }
  }
  spark.close()
}


class SolrToDelta2 extends App with LazyLogging {

  val filename = "solrToDelta.yaml"     //args(0)
  val ymlTxt = Source.fromFile(filename).mkString
  val yaml = ymlTxt.stripMargin.parseYaml
  val database = yaml.convertTo[Database]

  // Spark Session
  val spark = SparkSession.builder()
    .appName(database.jobName)
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  def sync(spark: SparkSession, database: Database, zookHost:String): Unit = {
    try {

      val dateFieldSolr = database.timestampLastColumn.getOrElse("")
      val dateFieldsDelta = database.timestampColumns.getOrElse(List())
      val dateMax = database.dateMax.getOrElse("")

      for(table <- database.tables.getOrElse(Nil)) {
        //if (table.isActive.getOrElse(true)) {

        val tableDelta = table.tableDelta.toString
        val pathDelta = table.schemaDelta.toString
        val tableSolr = table.tableSolr.toString
        val zkHost = zookHost   //table.ZkHost.toString
        val loadType = table.typeLoad.getOrElse("full")
        val pks = table.key

        val config = Map("S_TABLE_NAME" -> tableSolr, "S_TABLE_TYPE" -> "solr",
          "S_DATE_FIELD" -> dateFieldSolr, "ZKHOST" -> zkHost,

          "T_TABLE_NAME" -> tableDelta, "T_TABLE_TYPE" -> "delta",
          "PATH" -> pathDelta, "T_LOAD_TYPE" -> loadType, "T_DATE_MAX" -> dateMax
        )

        val sync = new Sync()
        sync.syncSourceTarget(spark, config, dateFieldsDelta, pks)

        //}
      }

    }
  }
  spark.close()
}

