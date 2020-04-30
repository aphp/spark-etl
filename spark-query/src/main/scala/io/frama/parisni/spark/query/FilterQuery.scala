package io.frama.parisni.spark.query

import org.apache.spark.sql.{Column, DataFrame}


case class FilterQuery(base: Query, filter: Column, as: String) extends QueryDecorator {
  lazy val df: DataFrame = base.df.where(filter)

  override def |(c: Column): FilterQuery = FilterQuery(base, filter && c, as)

  override def toString: String = s"""($base | $filter)"""
}

