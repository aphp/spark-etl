package io.frama.parisni.spark.meta.strategy.extractor

import org.apache.spark.sql.DataFrame

trait FeatureExtractTrait {
  def extractSource(df: DataFrame): DataFrame

  /**
    * Return a new dataframe with an added "is_pk" column
    *
    * @param df The input dataframe
    * @return the transformed dataframe
    */
  def extractPrimaryKey(df: DataFrame): DataFrame

  /**
    * Return a new dataframe with an added "is_fk" column
    *
    * @param df The input dataframe
    * @return the transformed dataframe
    */
  def extractForeignKey(df: DataFrame): DataFrame

  def inferForeignKey(df: DataFrame): DataFrame

}
