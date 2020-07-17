package io.frama.parisni.spark.sync.conf

import com.typesafe.scalalogging.LazyLogging
import io.frama.parisni.spark.dataframe.DFTool
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, greatest}

import scala.util.Try

object TargetConf extends LazyLogging {

  def getPostgresMaxDate(spark: SparkSession, tTableName: String, url: String, dateFields: List[String]): String = {

    val result = Try(dateFields.map(date => (date, spark.read.format("postgres")
      .option("url", url)
      .option("query", f"select max(${date}) + interval '1 second' from ${tTableName}")
      .load
      .first.get(0).toString
    )
    ).maxBy(_._2)._2)

    if (result.isSuccess) {
      logger.warn(s"found postgres max date : ${result}")
      result.getOrElse("")
    } else ""
  }

  def getSolrMaxDate(spark: SparkSession, tTableName: String, url: String, dateFields: List[String]): String = {

    val result = Try(dateFields.map(date => (date, spark.read.format("solr")
      .options(Map("collection" -> tTableName
        , "zkhost" -> url
        , "query" -> "*:*"
        , "max_rows" -> "1"
        , "request_handler" -> "/select"
        , "fields" -> date.toString
        , "solr.params" -> s"sort=${date} desc"
      ))
      .load
      .selectExpr(s"${date} + interval 1 second")
      .first.get(0).toString
    )
    ).maxBy(_._2)._2)

    if (result.isSuccess) {
      logger.warn(s"found solr max date : ${result}")
      result.get
    } else ""

  }

  def getDeltaMaxDate(spark: SparkSession, url: String, tTableName: String, dateFields: List[String]): String = {

    if (!DFTool.tableExists(spark, url, tTableName)) {
      logger.warn(s"table ${url}.${tTableName} does not exist")
      return ""
    }

    val result = DFTool.read(spark, url, tTableName, "delta")

    val maxDate = dateFields.size match {
      case 1 => col(dateFields(0))
      case _ => greatest(dateFields.map(x => col(x)): _*)
    }
    // get just date columns
    result.withColumn("MaxDate", maxDate)
      .selectExpr("max(MaxDate) + interval 1 second").first.get(0)
      .toString
  }
}

trait TargetConf extends LazyLogging {

  val T_TABLE_NAME: String = "T_TABLE_NAME"
  val T_TABLE_TYPE: String = "T_TABLE_TYPE" //"postgres", "solr", "delta", ....
  val T_DATE_MAX: String = "T_DATE_MAX"
  val T_LOAD_TYPE: String = "T_LOAD_TYPE"   //"full", "scd1", "scd2", ...
  //val T_LAST_UPDATE_FIELDS: String = "T_LAST_UPDATE_FIELDS"   // names of fields last_update_date in target table

  def getTargetTableName: Option[String]

  def getTargetTableType: Option[String]

  def getLoadType: Option[String]

  def getDateMax(spark: SparkSession): String = {
    ""
  }

  def checkTargetParams(config: Map[String, String]) = {

    require(config != null, "Config cannot be null")
    require(config.nonEmpty, "Config cannot be empty")

    require(config.get(T_LOAD_TYPE).isEmpty || (config.get(T_LOAD_TYPE).isDefined
      && ("full" :: "scd1" :: "scd2" :: Nil).contains(config.get(T_LOAD_TYPE).get)),
      "Loading type shall be in full, scd1, scd2")

    require(config.get(T_TABLE_TYPE).isEmpty || (config.get(T_TABLE_TYPE).isDefined
      && ("postgres" :: "solr" :: "delta" :: "parquet" :: Nil).contains(config.get(T_TABLE_TYPE).get)),
      "Target table shall be in postgres, solr, delta, parquet")
  }

  def calculDateMax(spark: SparkSession, url: String, tTableType: String, tTableName: String,
                    dateFields: List[String]): String = {

    val result = tTableType match {
      case "postgres" => TargetConf.getPostgresMaxDate(spark, tTableName, url, dateFields)
      /*{

        val result = dateFields.map(date => (date, spark.read.format("postgres")
          .option("url", url)
          .option("query", f"select max(${date}) + interval '1 second' from ${tTableName}")
          .load.first.get(0).toString
        )
        ).maxBy(_._2)._2
        logger.warn("PG: Max Date = " + result)
        result
      }*/
      case "delta" => TargetConf.getDeltaMaxDate(spark, url, tTableName, dateFields)
      case "solr" => TargetConf.getSolrMaxDate(spark, tTableName, url, dateFields)
    }
    result

  }
}
