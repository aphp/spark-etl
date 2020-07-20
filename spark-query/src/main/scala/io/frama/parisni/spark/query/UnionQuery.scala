package io.frama.parisni.spark.query

import org.apache.spark.sql.DataFrame

case class UnionQuery(
    top: Query,
    bottom: Query,
    as: String,
    byName: Boolean = true
) extends Query {
  override val joinAs: String = top.joinAs

  override lazy val df: DataFrame = {
    import compat._ // auto-fill for older spark
    if (byName) top.df.unionByName(bottom.df)
    else top.df.union(bottom.df)
  }

  override def nodes: Seq[Query] = List(top, bottom)

  override def toString: String = s"""($top & $bottom)"""
}

object UnionQuery {
  def apply(top: Query, bottom: Query): UnionQuery =
    UnionQuery(top, bottom, top.as)
}
