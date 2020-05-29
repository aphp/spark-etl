package io.frama.parisni.spark.sync.conf

import io.frama.parisni.spark.dataframe.DFTool
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try


class DeltaConf(config: Map[String, String], dates: List[String], pks: List[String])
  extends SourceAndTarget { //io.frama.parisni.spark.sync.conf.TargetConf with io.frama.parisni.spark.sync.conf.SourceConf with LazyLogging{

  checkTargetParams(config)
  checkSourceParams(config)
  // SourceTable fields & methods
  val PATH: String = "PATH" //PATH is used by Delta for connexion
  def getPath: Option[String] = config.get(PATH)

  def readSource(spark: SparkSession, path: String, s_table: String,
                 s_date_field: String, date_Max: String, load_type: String): DataFrame = {

    logger.warn("Reading data from Delta table ---------")

    var dfDelta = Try(DFTool.read(spark, path, s_table, "delta"))
      .getOrElse(spark.emptyDataFrame)

    if (load_type != "full" && date_Max != "")
      dfDelta = dfDelta.filter(f"${s_date_field} >= '${date_Max}'")

    dfDelta
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

    val result = config.get(T_DATE_MAX) match {
      case Some("") => calculDateMax(spark, getPath.getOrElse(""), getTargetTableType.getOrElse(""), getTargetTableName.getOrElse(""), getDateFields)
      case Some(_) => config.get(T_DATE_MAX).get
    }
    logger.warn(s"getting the maxdate : ${result}")
    result
  }

  /**
   * Delta Lake automatically validates that the schema of the DF being written is compatible with the schema of the target table.
   *  - All DataFrame columns must exist in the target table.
   *  - DataFrame column data types must match the column data types in the target table.
   *  - DataFrame column names cannot differ only by case.
   */
  def writeSource(spark: SparkSession
                  , s_df: DataFrame
                  , path: String
                  , t_table: String
                  , load_type: String
                  , hash_field: String = "hash"): Unit = {

    logger.warn("Writing data into Delta table ---------")

    //Add hash field to DF
    val hashedDF = DFTool.dfAddHash(s_df)

    load_type match {
      case "full" => DFTool.saveHive(hashedDF, DFTool.getDbTable(t_table, path), "delta")
      case "scd1" => DFTool.deltaScd1(hashedDF, t_table, getSourcePK, path)
    }
  }
}
