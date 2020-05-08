package io.frama.parisni.spark.sync.conf

import io.frama.parisni.spark.dataframe.DFTool
import org.apache.spark.sql.{DataFrame, SparkSession}


class ParquetConf(config: Map[String, String], dates: List[String], pks: List[String])
  extends SourceAndTarget {

  checkSourceParams(config)
  checkTargetParams(config)
  // SourceTable fields & methods
  val PATH: String = "PATH" //PATH is used by Parquet for connexion
  def getPath: Option[String] = config.get(PATH)

  def readSource(spark: SparkSession, path: String, s_table: String,
                 s_date_field: String, date_Max: String, load_type: String): DataFrame = {

    try {
      logger.warn("Reading data from Parquet table ---------")

      if (!checkTableExists(spark, path, s_table)) {
        logger.warn(s"Parquet Table ${s_table} doesn't exist")
        return spark.emptyDataFrame
      }

      val parquetPath = "%s/%s".format(path, s_table)
      var dfParquet = spark.read.format("parquet").load(parquetPath)

      if (load_type != "full")
        dfParquet = dfParquet.filter(f"${s_date_field} >= '${date_Max}'")

      dfParquet
    } catch {
      case re: RuntimeException => throw re
      case e: Exception => throw new RuntimeException(e)
    }
  }


  override def getSourceTableName = config.get(S_TABLE_NAME)

  override def getSourceTableType = config.get(S_TABLE_TYPE)

  override def getSourceDateField = config.get(S_DATE_FIELD)

  def getSourcePK = pks

  // TargetTable methods
  override def getTargetTableName = config.get(T_TABLE_NAME)

  override def getTargetTableType = config.get(T_TABLE_TYPE)

  override def getLoadType = config.get(T_LOAD_TYPE)

  def getDateFields = dates

  override def getDateMax(spark: SparkSession): String = {
    if (config.get(T_DATE_MAX).isDefined) config.get(T_DATE_MAX).getOrElse("")
    else if (!checkTableExists(spark, getPath.getOrElse(""), getTargetTableName.getOrElse(""))) {
      "1900-01-01 00:00:00"
    }
    else
      calculDateMax(spark, getPath.getOrElse(""), getTargetTableType.getOrElse(""), getTargetTableName.getOrElse(""), getDateFields)
  }

  def writeSource(spark: SparkSession, s_df: DataFrame, path: String, t_table: String, load_type: String,
                  hash_field: String = "hash"): Unit = {

    try {
      logger.warn("Writing data into Parquet table ---------")
      val parquetPath = "%s/%s".format(path, t_table)

      //Add hash field to DF
      val hashedDF = DFTool.dfAddHash(s_df)

      if (!checkTableExists(spark, path, t_table)) {
        logger.warn(s"Creating parquet table ${parquetPath} from scratch")
        hashedDF.write.format("parquet").save(parquetPath)
      }
      else {

        load_type match {
          case "full" => {
            hashedDF.write.format("parquet")
              .mode("overwrite")
              .save(parquetPath)
          }
          case "scd1" => {

            throw new Exception("Scd1 for parquet not implemented yet")

          }
        }
      }
    } catch {
      case re: RuntimeException => throw re
      case e: Exception => throw new RuntimeException(e)
    }
  }


  def checkTableExists(spark: SparkSession, path: String, table: String): Boolean = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val parquetPath = path + "/" + table
    val res = fs.exists(new org.apache.hadoop.fs.Path(parquetPath))
    logger.warn(s"Parquet Table ${parquetPath} exists = " + res)
    return res
  }

}
