package io.frama.parisni.spark.sync.copy

import com.typesafe.scalalogging.LazyLogging
import io.frama.parisni.spark.sync.Sync
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.SparkSession

import scala.io.Source
import PostgresToSolrYaml._

object SolrToPg extends App with LazyLogging {

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

    val hostPg = database.hostPg.toString
    val portPg = database.portPg.toString
    val databasePg = database.databasePg.toString
    val userPg = database.userPg.toString
    val dateFieldSolr = database.timestampLastColumn.getOrElse("")
    val dateFieldsPg = database.timestampColumns.getOrElse(List())
    val dateMax = database.dateMax.getOrElse("")

    for(table <- database.tables.getOrElse(Nil)) {
      //if (table.isActive.getOrElse(true)) {

        val schemaPg = table.schemaPg.toString
        val tablePg = table.tablePg.toString
        val zkHost = table.zkHost.toString
        val tableSolr = table.tableSolr.toString
        val loadType = table.typeLoad.getOrElse("full")
        val pks = table.key

        val config = Map("S_TABLE_NAME" -> tableSolr, "S_TABLE_TYPE" -> "solr",
          "S_DATE_FIELD" -> dateFieldSolr, "ZKHOST" -> zkHost,

          "T_TABLE_NAME" -> tablePg, "T_TABLE_TYPE" -> "postgres",
          "HOST" -> hostPg, "PORT" -> portPg, "DATABASE" -> databasePg,
          "USER" -> userPg, "SCHEMA" -> schemaPg, "T_LOAD_TYPE" -> loadType, "T_DATE_MAX" -> dateMax
        )

        val sync = new Sync()
        sync.syncSourceTarget(spark, config, dateFieldsPg, pks)

      //}
    }
  }
  spark.close()
}



class SolrToPg2 extends App with LazyLogging{

  val filename = "solrToPg.yaml"       //args(0)
  val ymlTxt = Source.fromFile(filename).mkString
  val yaml = ymlTxt.stripMargin.parseYaml
  val database = yaml.convertTo[Database]

  // SPARK PART
  val spark = SparkSession.builder()
    .appName(database.jobName)
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  def sync(spark: SparkSession, database: Database, port: String, zkHost:String): Unit = {
    try {

      val hostPg = database.hostPg.toString
      val portPg = port   //database.portPg.toString
      val databasePg = database.databasePg.toString
      val userPg = database.userPg.toString
      val dateFieldSolr = database.timestampLastColumn.getOrElse("")
      val dateFieldsPg = database.timestampColumns.getOrElse(List())
      val dateMax = database.dateMax.getOrElse("")    //2018-10-16 23:16:16

      for(table <- database.tables.getOrElse(Nil)) {
        //if (table.isActive.getOrElse(true)) {

        val schemaPg = table.schemaPg.toString
        val tablePg = table.tablePg.toString
        val zkHostSolr = zkHost   //table.zkHost.toString
        val tableSolr = table.tableSolr.toString
        val loadType = table.typeLoad.getOrElse("full")
        val pks = table.key

        val config = Map("S_TABLE_NAME" -> tableSolr, "S_TABLE_TYPE" -> "solr",
          "S_DATE_FIELD" -> dateFieldSolr, "ZKHOST" -> zkHostSolr,

          "T_TABLE_NAME" -> tablePg, "T_TABLE_TYPE" -> "postgres",
          "HOST" -> hostPg, "PORT" -> portPg, "DATABASE" -> databasePg,
          "USER" -> userPg, "SCHEMA" -> schemaPg, "T_LOAD_TYPE" -> loadType, "T_DATE_MAX" -> dateMax
        )

          val sync = new Sync()
          sync.syncSourceTarget(spark, config, dateFieldsPg, pks)

        //}
      }
    }
  }
  spark.close()
}
