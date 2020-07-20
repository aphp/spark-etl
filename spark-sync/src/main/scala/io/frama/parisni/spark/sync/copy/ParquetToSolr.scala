package io.frama.parisni.spark.sync.copy

import com.typesafe.scalalogging.LazyLogging

import io.frama.parisni.spark.sync.Sync
import io.frama.parisni.spark.sync.copy.ParquetToSolrYaml._
import org.apache.spark.sql.SparkSession

class ParquetToSolr extends LazyLogging {

  def sync(spark: SparkSession, database: Database, zookHost: String): Unit = {
    try {

      val dateFieldDelta = database.timestampLastColumn.getOrElse("")
      val dateFieldsSolr = database.timestampColumns.getOrElse(List())
      val dateMax = database.dateMax.getOrElse("")

      for (table <- database.tables.getOrElse(Nil)) {

        val tableDelta = table.tableParquet.toString
        val pathDelta = table.schemaParquet.toString
        val tableSolr = table.tableSolr.toString
        val zkHost = zookHost //table.ZkHost.toString
        val pks = table.key

        val config = Map(
          "S_TABLE_NAME" -> tableDelta,
          "S_TABLE_TYPE" -> "parquet",
          "S_DATE_FIELD" -> dateFieldDelta,
          "PATH" -> pathDelta,
          "T_TABLE_NAME" -> tableSolr,
          "T_TABLE_TYPE" -> "solr",
          "ZKHOST" -> zkHost,
          "T_DATE_MAX" -> dateMax //, "T_LOAD_TYPE" -> loadType
        )

        val sync = new Sync()
        sync.syncSourceTarget(spark, config, dateFieldsSolr, pks)

      }
    } catch {
      case re: RuntimeException => throw re
      case e: Exception         => throw new RuntimeException(e)
    }
  }
}
