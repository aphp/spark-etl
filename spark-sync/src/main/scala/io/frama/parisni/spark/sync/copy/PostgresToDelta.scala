package io.frama.parisni.spark.sync.copy

import com.typesafe.scalalogging.LazyLogging
import net.jcazevedo.moultingyaml._
import PostgresToDeltaYaml._
import io.frama.parisni.spark.sync.Sync
import org.apache.spark.sql.SparkSession

import scala.io.Source

object PostgresToDelta extends App with LazyLogging {

  val filename = args(0)
  val log = args(1)
  val ymlTxt = Source.fromFile(filename).mkString
  val yaml = ymlTxt.stripMargin.parseYaml
  val database = yaml.convertTo[Database]

  // Spark Session
  val spark = SparkSession.builder()
    .appName(database.jobName)
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel(args(1))

  try {

    val hostPg = database.hostPg.toString
    val portPg = database.portPg.toString
    val databasePg = database.databasePg.toString
    val userPg = database.userPg.toString
    val dateFieldPg = database.timestampLastColumn.getOrElse("")
    val dateFieldsDelta = database.timestampColumns.getOrElse(List())
    val dateMax = database.dateMax.getOrElse("")

    for (table <- database.tables.getOrElse(Nil)) {

      val schemaPg = table.schemaPg.toString
      val tablePg = table.tablePg.toString
      val pathDelta = table.schemaHive.toString
      val tableDelta = table.tableHive.toString
      val loadType = table.typeLoad.getOrElse("full")
      val pks = table.key

      val config = Map("S_TABLE_NAME" -> tablePg, "S_TABLE_TYPE" -> "postgres",
        "S_DATE_FIELD" -> dateFieldPg, "HOST" -> hostPg, "PORT" -> portPg,
        "DATABASE" -> databasePg, "USER" -> userPg, "SCHEMA" -> schemaPg,

        "T_TABLE_NAME" -> tableDelta, "T_TABLE_TYPE" -> "delta",
        "PATH" -> pathDelta, "T_LOAD_TYPE" -> loadType, "T_DATE_MAX" -> dateMax
      )

      val sync = new Sync()
      sync.syncSourceTarget(spark, config, dateFieldsDelta, pks)

    }
  } catch {
    case re: RuntimeException => throw re
    case e: Exception => throw new RuntimeException(e)
  } finally {
    spark.close()
  }
}


class PostgresToDelta extends LazyLogging {

  def sync(spark: SparkSession, database: Database, port: String): Unit = {
    try {

      val hostPg = database.hostPg.toString
      val portPg = port //database.portPg.toString
      val databasePg = database.databasePg.toString
      val userPg = database.userPg.toString
      val dateFieldPg = database.timestampLastColumn.getOrElse("")
      val dateFieldsDelta = database.timestampColumns.getOrElse(List())
      val dateMax = database.dateMax.getOrElse("")

      for (table <- database.tables.getOrElse(Nil)) {

        val schemaPg = table.schemaPg.toString
        val tablePg = table.tablePg.toString
        val pathDelta = table.schemaHive.toString
        val tableDelta = table.tableHive.toString
        val loadType = table.typeLoad.getOrElse("full")
        val pks = table.key

        val config = Map("S_TABLE_NAME" -> tablePg, "S_TABLE_TYPE" -> "postgres",
          "S_DATE_FIELD" -> dateFieldPg, "HOST" -> hostPg, "PORT" -> portPg,
          "DATABASE" -> databasePg, "USER" -> userPg, "SCHEMA" -> schemaPg,

          "T_TABLE_NAME" -> tableDelta, "T_TABLE_TYPE" -> "delta",
          "PATH" -> pathDelta, "T_LOAD_TYPE" -> loadType, "T_DATE_MAX" -> dateMax
        )

        val sync = new Sync()
        sync.syncSourceTarget(spark, config, dateFieldsDelta, pks)

      }
    } catch {
      case re: RuntimeException => throw re
      case e: Exception => throw new RuntimeException(e)
    }
  }
}
