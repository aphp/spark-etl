package io.frama.parisni.spark.query

import org.apache.spark.sql.{Column, DataFrame}


abstract class JoinQuery extends Query {
  val left: Query
  val right: Query
  val on: Column
  val joinType: String
  val joinOp: String
  val as: String

  lazy val df: DataFrame =
    AliasQuery.getAliasedDataFrame(left).join(AliasQuery.getAliasedDataFrame(right), on, joinType)

  override def leaves: Seq[Query] = left.leaves ++ right.leaves

  override def nodes: Seq[Query] = List(this, left, right)

  override def toString: String = s"""($left $joinOp $right.on$on)"""

  override def nodeString: String = super.nodeString + s" $on"
}

object JoinQuery {
  def unapply(j: JoinQuery): Option[(Query, Query)] = Some((j.left, j.right))

  // Guess column(s) to join on, up to 1 per left leave, based on leaves' joinAs:
  // on the left: id, {joinAs}_id, {as}_id
  // on the right: {left.joinAs}_id, {left.joinAs}_fk, {left.joinAs}
  val guesses: Seq[String => String] = Seq(_ + "_id", _ + "_fk", s => s)
  def guessJoinColumns(left: Query, rightColumns: Array[String]): Seq[JoinColumn] =
    for {
      left <- left.leaves
      rightCol <- guesses.map(_(left.joinAs)).find(rightColumns.contains)
      leftCol <- Seq("id", left.joinAs + "_id", left.as + "_id").find(left.df.columns.contains)
    } yield JoinColumn(left.as, leftCol, rightCol)
}


trait CanResolveJoinerTo[A] {
  def apply(left: Query, right: Query, on: Column): A

  // For `a + [joiner]` syntax
  def apply(left: Query, j: Joiner): A = {
    val resolved = j.resolve(left)
    apply(resolved.left, resolved.right, resolved.on)
  }

  // For `a + b`, `a + b.on("col")`, `SomeJoin(a, b)`, `SomeJoin(a, b, "col")` syntax
  def apply(left: Query, right: Query, columns: String *): A = columns match {
    case Nil => apply(left, AutoColumnsJoiner(right))
    case _   => apply(left, ColumnNameJoiner(right, columns :_*))
  }

  // For `SomeJoin(a, b, CommonColumnsJoiner)` syntax
  def apply(left: Query, right: Query, joinerBuilder: Query => Joiner): A = apply(left, joinerBuilder(right))
}


// left + right
case class InnerJoin(left: Query, right: Query, on: Column) extends JoinQuery {
  override val joinType: String = "inner"
  override val joinOp: String = "+"
  override val as = s"${left.as}__w_${right.as}"
}
object InnerJoin extends CanResolveJoinerTo[InnerJoin]


// left % right
case class LeftOuterJoin(left: Query, right: Query, on: Column) extends JoinQuery {
  override val joinType: String = "left_outer"
  override val joinOp: String = "%"
  override val as = s"${left.as}__w_${right.as}"
}
object LeftOuterJoin extends CanResolveJoinerTo[LeftOuterJoin]


// left %% right
case class FullOuterJoin(left: Query, right: Query, on: Column) extends JoinQuery {
  override val joinType: String = "full_outer"
  override val joinOp: String = "%%"
  override val as = s"${left.as}__w_${right.as}"
}
object FullOuterJoin extends CanResolveJoinerTo[FullOuterJoin]


// left - right
case class LeftAntiJoin(left: Query, right: Query, on: Column) extends JoinQuery {
  override val joinType: String = "left_anti"
  override val joinOp: String = "-"
  override val as = s"${left.as}__wo_${right.as}"
}
object LeftAntiJoin extends CanResolveJoinerTo[LeftAntiJoin]


// left ^ right
case class LeftSemiJoin(left: Query, right: Query, on: Column) extends JoinQuery {
  override val joinType: String = "left_semi"
  override val joinOp: String = "^"
  override val as = s"${left.as}__ex_${right.as}"
  override def leaves: Seq[Query] = left.leaves
}
object LeftSemiJoin extends CanResolveJoinerTo[LeftSemiJoin]


// left * right
case class CrossJoin(left: Query, right: Query) extends JoinQuery {
  override val on: Column = null
  override val joinType: String = "cross"
  override val joinOp: String = "*"
  override val as: String = s"${left.as}__by_${right.as}"
  override lazy val df: DataFrame = {
    import compat._ // auto-fill for older spark
    AliasQuery.getAliasedDataFrame(left).crossJoin(AliasQuery.getAliasedDataFrame(right))
  }
}


