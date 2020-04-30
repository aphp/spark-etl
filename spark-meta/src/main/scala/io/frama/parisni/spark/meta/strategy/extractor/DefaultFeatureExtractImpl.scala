package io.frama.parisni.spark.meta.strategy.extractor

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr, lit, regexp_extract, regexp_replace, when}

class DefaultFeatureExtractImpl extends FeatureExtractTrait {

  override def toString: String = "class DefaultFeatureExtractImpl extends FeatureExtractTrait"

  def extractSource(df: DataFrame): DataFrame = {
    df.withColumn("outil_source", regexp_extract(col("lib_table"), "^([^_]+)_.*$", 1))
      .withColumn("outil_source", when(col("outil_source").isin("orbis", "hegp", "mediweb", "phedra", "glims", "general", "arem", "ems"), col("outil_source")).otherwise("orbis"))
  }


  def extractPrimaryKey(df: DataFrame): DataFrame = {
    df.withColumn("is_pk", when((col("lib_column").rlike("^ids_")
      && col("order_column") === lit(1))
      || (col("lib_schema").rlike("omop")
      && col("lib_column").rlike("_id$")
      && col("order_column") === lit(1))
      , lit(true))
      .otherwise(lit(false)))
  }

  def extractForeignKey(df: DataFrame): DataFrame = {
    df.withColumn("is_fk"
      , when((col("lib_column").rlike("^ids_")
        && col("order_column") =!= lit(1))
        || (col("lib_schema").rlike("omop")
        && col("lib_column").rlike("_id$")
        && col("order_column") =!= lit(1)
        )
        , lit(true))
        .otherwise(lit(false)))
  }


  def inferForeignKey(df: DataFrame): DataFrame = {
    df.as("s")
      .filter(col("s.is_fk") === lit(true))
      .filter(!(col("s.lib_column") rlike "^ids_eds"))
      .join(df.as("t"), col("t.outil_source") === col("s.outil_source")
        && col("t.lib_table") =!= col("s.lib_table")
        && col("t.lib_schema") === col("s.lib_schema")
        && regexp_replace(col("t.lib_column"), "_ref_", "") === regexp_replace(col("s.lib_column"), "_ref_", "")
        && col("t.is_pk") === lit(true)
        , "left")
      .filter(col("t.lib_column").isNotNull)
      .union(
        df.as("s")
          .filter(col("s.is_fk") === lit(true))
          .filter(col("s.lib_schema").rlike("omop"))
          .join(df.as("t"),
            col("t.lib_table") =!= col("s.lib_table")
              && col("t.lib_schema") === col("s.lib_schema")
              && regexp_replace(col("s.lib_column"), ".*_concept_id$|concept_id_\\d$", "concept_id") ===
              col("t.lib_column")
              && col("t.is_pk") === lit(true)
            , "left")
          .filter(col("t.lib_column").isNotNull))
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
