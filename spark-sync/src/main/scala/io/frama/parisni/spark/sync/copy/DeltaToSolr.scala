package io.frama.parisni.spark.sync.copy

import com.typesafe.scalalogging.LazyLogging
import net.jcazevedo.moultingyaml._
import DeltaToSolrYaml._
import io.frama.parisni.spark.sync.Sync
import org.apache.spark.sql.SparkSession

import scala.io.Source

object DeltaToSolr extends App with LazyLogging {

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

    val dateFieldDelta = database.timestampLastColumn.getOrElse("")
    val dateFieldsSolr = database.timestampColumns.getOrElse(List())
    val dateMax = database.dateMax.getOrElse("")

    for (table <- database.tables.getOrElse(Nil)) {

      val tableDelta = table.tableDelta.toString
      val pathDelta = table.schemaDelta.toString
      val tableSolr = table.tableSolr.toString
      val zkHost = table.ZkHost.toString
      val loadType = table.typeLoad.getOrElse("full")
      val pks = table.key

      val config = Map("S_TABLE_NAME" -> tableDelta, "S_TABLE_TYPE" -> "delta",
        "S_DATE_FIELD" -> dateFieldDelta, "PATH" -> pathDelta,

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


class DeltaToSolr extends LazyLogging {

  def sync(spark: SparkSession, database: Database, zookHost: String): Unit = {
    try {

      val dateFieldDelta = database.timestampLastColumn.getOrElse("")
      val dateFieldsSolr = database.timestampColumns.getOrElse(List())
      val dateMax = database.dateMax.getOrElse("")

      for (table <- database.tables.getOrElse(Nil)) {

        val tableDelta = table.tableDelta.toString
        val pathDelta = table.schemaDelta.toString
        val tableSolr = table.tableSolr.toString
        val zkHost = zookHost //table.ZkHost.toString
        val pks = table.key

        val config = Map("S_TABLE_NAME" -> tableDelta, "S_TABLE_TYPE" -> "delta",
          "S_DATE_FIELD" -> dateFieldDelta, "PATH" -> pathDelta,

          "T_TABLE_NAME" -> tableSolr, "T_TABLE_TYPE" -> "solr",
          "ZKHOST" -> zkHost, "T_DATE_MAX" -> dateMax //, "T_LOAD_TYPE" -> "full"
        )

        val sync = new Sync()
        sync.syncSourceTarget(spark, config, dateFieldsSolr, pks)

      }
    } catch {
      case re: RuntimeException => throw re
      case e: Exception => throw new RuntimeException(e)
    }
  }
}

