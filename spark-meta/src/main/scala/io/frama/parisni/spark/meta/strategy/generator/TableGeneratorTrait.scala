package io.frama.parisni.spark.meta.strategy.generator

import org.apache.spark.sql.DataFrame

trait TableGeneratorTrait {
  def generateDatabase(df: DataFrame): DataFrame

  def generateSchema(df: DataFrame): DataFrame

  def generateTable(df: DataFrame): DataFrame

  def generateColumn(df: DataFrame): DataFrame
}
