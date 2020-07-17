package io.frama.parisni.spark.sync.copy

import com.typesafe.scalalogging.LazyLogging
import net.jcazevedo.moultingyaml._
import PostgresToSolrYaml._
import io.frama.parisni.spark.sync.Sync
import org.apache.spark.sql.SparkSession

import scala.io.Source

object PostgresToSolr extends App with LazyLogging {

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

    val hostPg = database.hostPg.toString
    val portPg = database.portPg.toString
    val databasePg = database.databasePg.toString
    val userPg = database.userPg.toString
    val dateFieldPg = database.timestampLastColumn.getOrElse("")
    val dateFieldsSolr = database.timestampColumns.getOrElse(List())
    val dateMax = database.dateMax.getOrElse("")

    for(table <- database.tables.getOrElse(Nil)) {

        val schemaPg = table.schemaPg.toString
        val tablePg = table.tablePg.toString
        val zkHost = table.zkHost.toString
        val tableSolr = table.tableSolr.toString
        val loadType = table.typeLoad.getOrElse("full")
        val pks = table.key

        val config = Map("S_TABLE_NAME" -> tablePg, "S_TABLE_TYPE" -> "postgres",
          "S_DATE_FIELD" -> dateFieldPg, "HOST" -> hostPg, "PORT" -> portPg,
          "DATABASE" -> databasePg, "USER" -> userPg, "SCHEMA" -> schemaPg,

          "T_TABLE_NAME" -> tableSolr, "T_TABLE_TYPE" -> "solr",
          "ZKHOST" -> zkHost, "T_LOAD_TYPE" -> loadType, "T_DATE_MAX" -> dateMax
        )

        val sync = new Sync()
        sync.syncSourceTarget(spark, config, dateFieldsSolr, pks)

    }
  } catch {
    case re: RuntimeException => throw re
    case e: Exception => throw new RuntimeException(e)
  } finally {
    spark.close()
  }
}



class PostgresToSolr2 extends App with LazyLogging{

  val filename = "postgresToSolr.yaml"
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
      val dateFieldPg = database.timestampLastColumn.getOrElse("")
      val dateFieldsSolr = database.timestampColumns.getOrElse(List())
      val dateMax = database.dateMax.getOrElse("")

      for(table <- database.tables.getOrElse(Nil)) {

          val schemaPg = table.schemaPg.toString
          val tablePg = table.tablePg.toString
          val zkHostSolr = zkHost   //table.zkHost.toString
          val tableSolr = table.tableSolr.toString
          val loadType = table.typeLoad.getOrElse("full")
          val pks = table.key

          val config = Map("S_TABLE_NAME" -> tablePg, "S_TABLE_TYPE" -> "postgres",
            "S_DATE_FIELD" -> dateFieldPg, "HOST" -> hostPg, "PORT" -> portPg,
            "DATABASE" -> databasePg, "USER" -> userPg, "SCHEMA" -> schemaPg,

            "T_TABLE_NAME" -> tableSolr, "T_TABLE_TYPE" -> "solr",
            "ZKHOST" -> zkHostSolr, "T_LOAD_TYPE" -> loadType, "T_DATE_MAX" -> dateMax
          )

          val sync = new Sync()
          sync.syncSourceTarget(spark, config, dateFieldsSolr, pks)

      }
    } catch {
      case re: RuntimeException => throw re
      case e: Exception => throw new RuntimeException(e)
    } finally {
      spark.close()
    }
  }
}
