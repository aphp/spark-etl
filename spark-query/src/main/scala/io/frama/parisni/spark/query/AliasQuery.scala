package io.frama.parisni.spark.query

import org.apache.spark.sql.{Column, DataFrame}

// Notably allows filtering on both sides of self join:
// p = q.alias("parent")
// joined = p + q.on("id" -> "parent_id")
// joined | p("id") < q("id")
case class AliasQuery(base: Query, as: String) extends QueryDecorator {
  override lazy val df: DataFrame = base.df.as(as)

  import org.apache.spark.sql.functions.col
  override def apply(column: String): Column =
    if (df.columns.count(_ == column) == 1) col(as + "." + column)
    else super.apply(column)

  override def toString: String = {
    val b = base.toString
    if (b == as) b
    else s"""$b.as("$as")"""
  }
}

object AliasQuery {
  def apply(q: Query): AliasQuery = apply(q.as)(q)
  def apply(as: String)(q: Query): AliasQuery =
    q match {
      case AliasQuery(base, _) => AliasQuery(base, as)
      case _                   => AliasQuery(q, as)
    }

  def getAliasedDataFrame(q: Query): DataFrame =
    q match {
      case AliasQuery(_, _) => q.df
      case _                => q.df.as(q.as)
    }
}
