package io.frama.parisni.spark.sync.conf

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{greatest, max}

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

  //def getDateFields: Option[List[String]]

  def calculDateMax(spark: SparkSession, url: String, t_table_type: String, t_table_name: String,
                    date_fields: List[String]): String = {

    t_table_type match {
      case "postgres" => {

        val result = date_fields.map(date => (date, spark.read.format("postgres")
          .option("url", url)
          .option("query", f"select max(${date}) from ${t_table_name}")
          .load.first.get(0).toString
        )
        ).maxBy(_._2)._2
        logger.warn("PG: Max Date = " + result)
        result
      }
      case "delta" => {

        val deltaPath = "%s/%s".format(url, t_table_name) //Url = Path
        val deltaDF = spark.read.format("delta").load(deltaPath)

        // get just date columns
        val newDF = deltaDF.select(date_fields.head, date_fields.tail: _*) //"date_update", "date_update2", "date_update3")
        val numCols = newDF.columns.tail
        val result = deltaDF.withColumn("MaxDate", greatest(numCols.head, numCols.tail: _*))
          .select(max("MaxDate")).first.get(0)

        logger.warn("Delta: Max Date = " + result)
        result.toString
      }
      case "solr" => {

        //TODO: create a loop on every fields
        val options = Map("collection" -> t_table_name
          , "zkhost" -> url
          , "query" -> "*:*"
          , "solr.params" -> s"sort: ${date_fields.head} desc"
          , "fields" -> date_fields.mkString(",")
          , "rows" -> "1"
        )

        val solrDF = spark.read.format("solr").options(options)
          .load

        // get just date columns
        val newDF = solrDF.select(date_fields.head, date_fields.tail: _*) //"date_update", "date_update2", "date_update3")
        val numCols = newDF.columns.tail
        val result = solrDF.withColumn("MaxDate", greatest(numCols.head, numCols.tail: _*))
          .select(max("MaxDate")).first.get(0)

        logger.warn("Solr: Max Date = " + result)
        result.toString

      }
    }
  }

  //def writeSource(s_df: DataFrame, strings: String*): Unit
}
