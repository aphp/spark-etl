package io.frama.parisni.spark.sync

import com.typesafe.scalalogging.LazyLogging
import io.frama.parisni.spark.sync.conf.{DeltaConf, ParquetConf, PostgresConf, SolrConf}
import org.apache.spark.sql.SparkSession

class Sync extends LazyLogging {

  def syncSourceTarget(spark: SparkSession, config: Map[String, String], dates: List[String],
                       pks: List[String]): Unit = {

    val sourceType = config.get("S_TABLE_TYPE").getOrElse("")
    val targetType = config.get("T_TABLE_TYPE").getOrElse("")
    var sourceDF = spark.emptyDataFrame

    val pgc = new PostgresConf(config, dates, pks)
    val dc = new DeltaConf(config, dates, pks)
    val slc = new SolrConf(config, dates, pks)
    val plc = new ParquetConf(config, dates, pks)

    /** Calculation of dateMax: be sure that "target" table exists
     * (in order to calculate dateMax) or provide dateMax **/
    var dateMax = ""
    if (targetType == "postgres") dateMax = pgc.getDateMax(spark)
    else if (targetType == "delta") dateMax = dc.getDateMax(spark)
    else if (targetType == "solr") dateMax = slc.getDateMax(spark)
    else if (targetType == "parquet") dateMax = plc.getDateMax(spark)

    // Load table "source" as DF
    sourceType match {
      case "postgres" => {

        val host = pgc.getHost.getOrElse("localhost")
        val port = pgc.getPort.getOrElse("5432")
        val db = pgc.getDB.getOrElse("postgres")
        val user = pgc.getUser.getOrElse("postgres")
        val schema = pgc.getSchema.getOrElse("public")
        val sTable = pgc.getSourceTableName.getOrElse("")
        val sDateField = pgc.getSourceDateField.getOrElse("")
        val loadType = pgc.getLoadType.getOrElse("")

        sourceDF = pgc.readSource(spark, host, port, db, user, schema, sTable, sDateField, dateMax, loadType, pks)
      }
      case "delta" => {

        val path = dc.getPath.getOrElse("/tmp")
        val sTable = dc.getSourceTableName.getOrElse("")
        val sDateField = dc.getSourceDateField.getOrElse("")
        val loadType = dc.getLoadType.getOrElse("")

        sourceDF = dc.readSource(spark, path, sTable, sDateField, dateMax, loadType)
      }
      case "parquet" => {

        val path = dc.getPath.getOrElse("/tmp")
        val sTable = dc.getSourceTableName.getOrElse("")
        val sDateField = dc.getSourceDateField.getOrElse("")
        val loadType = dc.getLoadType.getOrElse("")

        sourceDF = plc.readSource(spark, path, sTable, sDateField, dateMax, loadType)
      }
      case "solr" => {

        val zkhost = slc.getZkHost.getOrElse("")
        val sCollection = slc.getSourceTableName.getOrElse("")
        val sDateField = slc.getSourceDateField.getOrElse("")

        sourceDF = slc.readSource(spark, zkhost, sCollection, sDateField, dateMax)
      }
    }

    val candidateCount: Long = sourceDF.count
    logger.warn(s"rows to modify / insert ${candidateCount}")

    if (candidateCount > 0) {
      // Merge DF with table "target"
      targetType match {
        case "postgres" => {

          val host = pgc.getHost.getOrElse("localhost")
          val port = pgc.getPort.getOrElse("5432")
          val db = pgc.getDB.getOrElse("postgres")
          val user = pgc.getUser.getOrElse("postgres")
          val schema = pgc.getSchema.getOrElse("public")
          val tTable = pgc.getTargetTableName.getOrElse("")
          val loadType = pgc.getLoadType.getOrElse("")
          val hashField = pgc.getSourcePK.mkString(",")

          pgc.writeSource(spark, sourceDF, host, port, db, user, schema, tTable, loadType, hashField)
        }

        case "delta" => {

          val path = dc.getPath.getOrElse("/tmp")
          val tTable = dc.getTargetTableName.getOrElse("")
          val loadType = dc.getLoadType.getOrElse("")

          dc.writeSource(spark, sourceDF, path, tTable, loadType)
        }
        case "parquet" => {

          val path = dc.getPath.getOrElse("/tmp")
          val tTable = dc.getTargetTableName.getOrElse("")
          val loadType = dc.getLoadType.getOrElse("")

          plc.writeSource(spark, sourceDF, path, tTable, loadType)
        }
        case "solr" => {

          val zkhost = slc.getZkHost.getOrElse("")
          val tCollection = slc.getTargetTableName.getOrElse("")

          slc.writeSource(sourceDF, zkhost, tCollection)
        }
      }
    }
  }
}
