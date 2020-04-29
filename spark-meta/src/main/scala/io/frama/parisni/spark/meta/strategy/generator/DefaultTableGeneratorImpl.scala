package io.frama.parisni.spark.meta.strategy.generator

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit


class DefaultTableGeneratorImpl extends TableGeneratorTrait {

  override def toString: String = "class DefaultTableGeneratorImpl extends TableGeneratorTrait"

  def generateDatabase(df: DataFrame): DataFrame = {
    df.dropDuplicates("lib_database")
      .select("lib_database")
  }

  def generateSchema(df: DataFrame): DataFrame = {
    df.dropDuplicates("lib_database", "lib_schema")
      .withColumn("is_technical",
        lit(true))
      .select("lib_database", "lib_schema", "is_technical")
  }

  //  def generateSchema(df: DataFrame): DataFrame = {
  //    df.dropDuplicates("lib_database", "lib_schema")
  //      .withColumn("is_technical", when(expr(
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
  //      .select("lib_database", "lib_schema", "is_technical")
  //  }


  def generateTable(df: DataFrame): DataFrame = {
    df.dropDuplicates("lib_database", "lib_schema", "lib_table")
      .select("lib_database", "lib_schema", "lib_table", "outil_source"
        , "count_table", "last_analyze", "typ_table", "last_commit_timestampz")
  }

  def generateColumn(df: DataFrame): DataFrame = {
    val result = df.dropDuplicates("lib_database", "lib_schema", "lib_table", "lib_column")
      .selectExpr("lib_database", "lib_schema",
        "lib_table", "lib_column", "typ_column", "order_column",
        "is_mandatory", "is_pk", "is_fk", "is_index",
        "null_ratio_column", "count_distinct_column",
        "comment_fonctionnel_column as comment_fonctionnel")
    result
  }

}
