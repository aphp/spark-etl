package io.frama.parisni.spark.meta.extractor

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class OmopFeatureExtractImpl extends FeatureExtractTrait {

  override def toString: String = "class DefaultFeatureExtractImpl extends FeatureExtractTrait"

  def extractSource(df: DataFrame): DataFrame = {
    df.withColumn("outil_source", lit("omop"))
  }


  def extractPrimaryKey(df: DataFrame): DataFrame = {
    df.withColumn("is_pk", when((
      col("lib_schema").rlike("omop")
        && col("lib_column").rlike("_id$")
        && col("order_column") === lit(1)
      ), lit(true))
      .otherwise(lit(false)))
  }

  def extractForeignKey(df: DataFrame): DataFrame = {
    df.withColumn("is_fk"
      , when((
        col("lib_schema").rlike("omop")
          && col("lib_column").rlike("_id$|_id_[0-9]$")
          && col("order_column") =!= lit(1)
        ), lit(true))
        .otherwise(lit(false)))
  }


  def inferForeignKey(df: DataFrame): DataFrame = {
    df.as("s")
      .filter(col("s.is_fk") === lit(true))
    df.as("s")
      .filter(col("s.is_fk") === lit(true))
      .join(df.as("t"),
        col("t.lib_table") =!= col("s.lib_table")
          && col("t.lib_schema") === col("s.lib_schema")
          && regexp_replace(col("s.lib_column"), ".*_concept_id$|concept_id_[0-9]$", "concept_id")
          ===
          col("t.lib_column")
          && col("t.is_pk") === lit(true)
        , "left")
      .filter(col("t.lib_column").isNotNull)
      .selectExpr(
        "s.lib_database lib_database_source"
        , "s.lib_schema lib_schema_source"
        , "s.lib_table lib_table_source"
        , "s.lib_column lib_column_source"
        , "t.lib_database lib_database_target"
        , "t.lib_schema as lib_schema_target"
        , "t.lib_table as lib_table_target"
        , "t.lib_column as lib_column_target"
        , "'FK' as lib_reference")
  }


  def generateDatabase(df: DataFrame): DataFrame = {
    df.dropDuplicates("lib_database")
      .select("lib_database")
  }

  def generateSchema(df: DataFrame): DataFrame = {
    df.dropDuplicates("lib_database", "lib_schema")
      .withColumn("is_technical", lit(true))
      .select("lib_database", "lib_schema", "is_technical")
  }


  def generateTable(df: DataFrame): DataFrame = {
    df.dropDuplicates("lib_database", "lib_schema", "lib_table")
      .select("lib_database", "lib_schema", "lib_table", "outil_source", "count_table", "last_analyze", "typ_table")
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
