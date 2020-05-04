package io.frama.parisni.spark.meta.strategy.extractor

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class I2b2FeatureExtractImpl extends FeatureExtractTrait {

  override def toString: String = "class I2b2FeatureExtractImpl extends FeatureExtractTrait"

  def extractSource(df: DataFrame): DataFrame = {
    df.withColumn("outil_source", lit("i2b2"))
  }


  def extractPrimaryKey(df: DataFrame): DataFrame = {
    df.withColumn("is_pk", when((
      col("lib_column").rlike("_num$|concept_cd$")
        && col("order_column") === lit(1)
      ), lit(true))
      .otherwise(lit(false)))
  }

  def extractForeignKey(df: DataFrame): DataFrame = {
    df.withColumn("is_fk"
      , when((
        col("lib_column").rlike("_num$|concept_cd$")
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
          && col("s.lib_column") === col("t.lib_column")
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

}
