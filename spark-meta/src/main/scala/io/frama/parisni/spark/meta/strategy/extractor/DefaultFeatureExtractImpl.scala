package io.frama.parisni.spark.meta.strategy.extractor

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{
  col,
  lit,
  regexp_extract,
  regexp_replace,
  when
}
import io.frama.parisni.spark.meta.Constants._

class DefaultFeatureExtractImpl extends FeatureExtractTrait {

  override def toString: String =
    "class DefaultFeatureExtractImpl extends FeatureExtractTrait"

  def extractSource(df: DataFrame): DataFrame = {
    df.withColumn(
      OUTIL_SOURCE,
      regexp_extract(col("lib_table"), "^([^_]+)_.*$", 1)
    ).withColumn(
      OUTIL_SOURCE,
      when(
        col(OUTIL_SOURCE).isin(
          "orbis",
          "hegp",
          "mediweb",
          "phedra",
          "glims",
          "general",
          "arem",
          "ems"
        ),
        col(OUTIL_SOURCE)
      ).otherwise("orbis")
    )
  }

  def extractPrimaryKey(df: DataFrame): DataFrame = {
    df.withColumn(
      IS_PK,
      when(
        (col(LIB_COLUMN).rlike("^ids_")
          && col(ORDER_COLUMN) === lit(1))
          || (col(LIB_SCHEMA).rlike("omop")
            && col(LIB_COLUMN).rlike("_id$")
            && col(ORDER_COLUMN) === lit(1)),
        lit(true)
      )
        .otherwise(lit(false))
    )
  }

  def extractForeignKey(df: DataFrame): DataFrame = {
    df.withColumn(
      IS_FK,
      when(
        (col(LIB_COLUMN).rlike("^ids_")
          && col(ORDER_COLUMN) =!= lit(1))
          || (col(LIB_SCHEMA).rlike("omop")
            && col(LIB_COLUMN).rlike("_id$")
            && col(ORDER_COLUMN) =!= lit(1)),
        lit(true)
      )
        .otherwise(lit(false))
    )
  }

  def inferForeignKey(df: DataFrame): DataFrame = {
    df.as("s")
      .filter(col("s.is_fk") === lit(true))
      .filter(!(col("s.lib_column") rlike "^ids_eds"))
      .join(
        df.as("t"),
        col("t.outil_source") === col("s.outil_source")
          && col("t.lib_table") =!= col("s.lib_table")
          && col("t.lib_schema") === col("s.lib_schema")
          && regexp_replace(
            col("t.lib_column"),
            "_ref_",
            ""
          ) === regexp_replace(col("s.lib_column"), "_ref_", "")
          && col("t.is_pk") === lit(true),
        "left"
      )
      .filter(col("t.lib_column").isNotNull)
      .union(
        df.as("s")
          .filter(col("s.is_fk") === lit(true))
          .filter(col("s.lib_schema").rlike("omop"))
          .join(
            df.as("t"),
            col("t.lib_table") =!= col("s.lib_table")
              && col("t.lib_schema") === col("s.lib_schema")
              && regexp_replace(
                col("s.lib_column"),
                ".*_concept_id$|concept_id_\\d$",
                "concept_id"
              ) ===
                col("t.lib_column")
              && col("t.is_pk") === lit(true),
            "left"
          )
          .filter(col("t.lib_column").isNotNull)
      )
      .selectExpr(
        "s.lib_database lib_database_source",
        "s.lib_schema lib_schema_source",
        "s.lib_table lib_table_source",
        "s.lib_column lib_column_source",
        "t.lib_database lib_database_target",
        "t.lib_schema as lib_schema_target",
        "t.lib_table as lib_table_target",
        "t.lib_column as lib_column_target",
        "'FK' as lib_reference"
      )
  }

}
