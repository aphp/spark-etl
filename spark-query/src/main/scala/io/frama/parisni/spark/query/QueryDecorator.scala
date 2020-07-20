package io.frama.parisni.spark.query

import org.apache.spark.sql.Column

trait QueryDecorator extends Query {
  val base: Query
  override def leaves: Seq[Query] = base.leaves
  override def nodes: Seq[Query] = List(base)
  override def apply(column: String): Column = base.apply(column)
}

object QueryDecorator {
  def unapply(q: QueryDecorator): Option[Query] = Some(q.base)
}
