package io.frama.parisni.spark.sync.conf

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, greatest}

import scala.util.Try

object TargetConf extends LazyLogging {
  def getSolrMaxDate(spark: SparkSession, t_table_name: String, url: String, date_fields: List[String]): String = {

    val result = Try(date_fields.map(date => (date, spark.read.format("solr")
      .options(Map("collection" -> t_table_name
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
}

trait TargetConf extends LazyLogging {

  val T_TABLE_NAME: String = "T_TABLE_NAME"
  val T_TABLE_TYPE: String = "T_TABLE_TYPE" //"postgres", "solr", "delta", ....
  val T_DATE_MAX: String = "T_DATE_MAX"
  val T_LOAD_TYPE: String = "T_LOAD_TYPE" //"full", "scd1", "scd2", ...
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

  def calculDateMax(spark: SparkSession, url: String, t_table_type: String, t_table_name: String,
                    date_fields: List[String]): String = {

    val result = t_table_type match {
      case "postgres" => {

        val result = date_fields.map(date => (date, spark.read.format("postgres")
          .option("url", url)
          .option("query", f"select max(${date}) + interval '1 second' from ${t_table_name}")
          .load.first.get(0).toString
        )
        ).maxBy(_._2)._2
        logger.warn("PG: Max Date = " + result)
        result
      }
      case "delta" => {

        val deltaPath = "%s/%s".format(url, t_table_name) //Url = Path
        val result = spark.read.format("delta").load(deltaPath)

        val maxDate = date_fields.size match {
          case 1 => col(date_fields(0))
          case _ => greatest(date_fields.map(x => col(x)): _*)
        }
        // get just date columns
        result.withColumn("MaxDate", maxDate)
          .selectExpr("max(MaxDate) + interval 1 second").first.get(0)
          .toString

      }
      case "solr" => TargetConf.getSolrMaxDate(spark, t_table_name, url, date_fields)
    }
    result

  }
}
