package io.frama.parisni.spark.meta.strategy.generator

import io.frama.parisni.spark.meta.Constants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit


class DefaultTableGeneratorImpl extends TableGeneratorTrait {

  override def toString: String = "class DefaultTableGeneratorImpl extends TableGeneratorTrait"

  def generateDatabase(df: DataFrame): DataFrame = {
    df.dropDuplicates(LIB_DATABASE)
      .select(LIB_DATABASE)
  }

  def generateSchema(df: DataFrame): DataFrame = {
    df.dropDuplicates(LIB_DATABASE, LIB_SCHEMA)
      .withColumn(IS_TECHNICAL, lit(true))
      .select(LIB_DATABASE, LIB_SCHEMA, IS_TECHNICAL)
  }

  //  def generateSchema(df: DataFrame): DataFrame = {
  //    df.dropDuplicates(LIB_DATABASE, LIB_SCHEMA)
  //      .withColumn(IS_TECHNICAL, when(expr(
  //        """
  //          (lib_database = 'pg-prod' AND lib_schema in ('eds'))
  //          OR  (lib_database = 'pg-prod' AND lib_schema in ('eds'))
  //          OR  (lib_database = 'eds-qua' AND lib_schema in ('eds'))
  //          OR  (lib_database = 'edsp-prod' AND lib_schema in ('edsp'))
  //          OR  (lib_database = 'spark-prod' AND lib_schema in ('edsprod', 'omop_prod', 'omop'))
  //          OR  (lib_database = 'omop-prod' AND lib_schema rlike ('omop'))
  //          """)
  //        , lit(false))
  //        .otherwise(lit(true)))
  //      .select(LIB_DATABASE, LIB_SCHEMA, IS_TECHNICAL)
  //  }


  def generateTable(df: DataFrame): DataFrame = {
    df.dropDuplicates(LIB_DATABASE, LIB_SCHEMA, LIB_TABLE)
      .select(LIB_DATABASE, LIB_SCHEMA, LIB_TABLE, OUTIL_SOURCE
        , COUNT_TABLE, LAST_ANALYSE, TYP_TABLE, LAST_COMMIT_TIMESTAMPZ)
  }

  def generateColumn(df: DataFrame): DataFrame = {
    df.dropDuplicates(LIB_DATABASE, LIB_SCHEMA, LIB_TABLE, LIB_COLUMN)
      .selectExpr(LIB_DATABASE, LIB_SCHEMA,
        LIB_TABLE, LIB_COLUMN, TYP_COLUMN, ORDER_COLUMN,
        IS_MANDATORY, IS_PK, IS_FK, IS_INDEX,
        NULL_RATIO_COLUMN, COUNT_DISTINCT_COLUMN,
        s"$COMMENT_FONCTIONNEL_COLUMN as $COMMENT_FONCTIONNEL")
  }

}
