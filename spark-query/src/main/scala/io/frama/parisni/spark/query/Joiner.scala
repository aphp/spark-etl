package io.frama.parisni.spark.query

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StructField


trait Joiner {
  val right: Query
  def resolve(left: Query): ResolvedJoin
}

case class ResolvedJoin(left: Query, right: Query, on: Column)


// left + right.on(left("leftCol") === right("rightCol"))
// joins through column expression
case class ColumnJoiner(right: Query, column: Column) extends Joiner {
  override def resolve(left: Query): ResolvedJoin = ResolvedJoin(left, right, column)
}


// joins via column names
trait NameJoiner extends Joiner {
  // name resolution delegated to implementor
  def resolveColumns(left: Query): Seq[JoinColumn]

  override def resolve(left: Query): ResolvedJoin = {
    val aliasedRight = AliasQuery(right.as)(right)
    val aliasedLeft = AliasQuery(left.as)(left)
    val candidates = resolveColumns(left)
    val tried = collection.mutable.Set.empty[JoinColumn]
    val cols = for {
      j <- candidates if tried.add(j)
      lq <- left.leaves.find(_.as == j.leftAlias)
      lcol = (if (lq == left) aliasedLeft else lq)(j.leftColumn)
      rcol = aliasedRight(j.rightColumn)
    } yield lcol === rcol
    cols match {
      case Nil => throw new IllegalArgumentException(
        s"No column found to join ${left.as} <-> ${right.as}, " +
          s"strategy=${getClass.getSimpleName}, tried ${candidates.mkString("[", ", ", "]")}")
      case _   => ResolvedJoin(aliasedLeft, aliasedRight, cols.reduce(_ && _))
    }
  }
}


case class JoinColumn(leftAlias: String, leftColumn: String, rightAlias: String, rightColumn: String)

object JoinColumn {
  def apply(leftAlias: String, rightAlias: String, column: String): JoinColumn =
    JoinColumn(leftAlias, column, rightAlias, column)
}


// left + right
// Guess column(s) to join on, up to 1 per leave, based on leaves' joinAs:
// on one side `q`: id, {q.joinAs}_id, {q.as}_id
// on the other side: {q.joinAs}_id, {q.joinAs}_fk, {q.joinAs}
case class AutoColumnsJoiner(right: Query) extends NameJoiner {
  override def resolveColumns(left: Query): Seq[JoinColumn] = {
    val ltr = AutoColumnsJoiner.guessJoinColumns(left, right)
    // reverse search
    val rtl = AutoColumnsJoiner.guessJoinColumns(right, left).map {
      case JoinColumn(rightAlias, rightCol, leftAlias, leftCol) =>
        JoinColumn(leftAlias, leftCol, rightAlias, rightCol)
    }
    ltr ++ rtl
  }
}

object AutoColumnsJoiner {
  // left + right
  // Guess column(s) to join on, up to 1 per left leave, based on leaves' joinAs:
  // on the left: id, {joinAs}_id, {as}_id
  // on the right: {left.joinAs}_id, {left.joinAs}_fk, {left.joinAs}
  val guesses: Seq[String => String] = Seq(_ + "_id", _ + "_fk", s => s)
  def guessJoinColumns(left: Query, right: Query): Seq[JoinColumn] =
    for {
      left <- left.leaves
      rightCol <- guesses.map(_(left.joinAs)).find(right.df.columns.contains)
      leftCol <- (left.idFields ++ Seq(left.joinAs + "_id", left.as + "_id")).find(left.df.columns.contains)
      right <- right.leaves.find(_.df.columns.contains(rightCol))
    } yield JoinColumn(left.as, leftCol, right.as, rightCol)
}


// left + ~right
// joins on common columns in schema
case class CommonColumnsJoiner(right: Query) extends NameJoiner {
  override def resolveColumns(left: Query): Seq[JoinColumn] = {
    val cols = collection.mutable.Set.empty[StructField]
    for {
      l <- left.leaves
      col <- l.df.schema if right.df.schema.contains(col) && cols.add(col)
    } yield JoinColumn(l.as, right.as, col.name)
  }
}


// left + right.on("col", ...)
case class ColumnNameJoiner(right: Query, onCols: String *) extends NameJoiner {
  require(onCols.forall(right.df.columns.contains), "Some join columns not found in right query")

  override def resolveColumns(left: Query): Seq[JoinColumn] =
    for {
      col <- onCols
      left <- left.leaves if left.df.columns.contains(col)
    } yield JoinColumn(left.as, col, right.as, col)
}


// left + right.on("leftCol" -> "rightCol", ...)
case class ColumnNamesJoiner(right: Query, onCols: (String, String) *) extends NameJoiner {
  require(onCols.forall(on => right.df.columns.contains(on._2)), "Some join columns not found in right query")

  override def resolveColumns(left: Query): Seq[JoinColumn] =
    for {
      (lCol, rCol) <- onCols
      left <- left.leaves if left.df.columns.contains(lCol)
    } yield JoinColumn(left.as, lCol, right.as, rCol)
}


// left + right.on(left -> "col", ...)
case class AliasAndColumnNameJoiner(right: Query, onCols: (String, String) *) extends NameJoiner {
  require(onCols.forall(on => right.df.columns.contains(on._2)), "Some join columns not found in right query")

  override def resolveColumns(left: Query): Seq[JoinColumn] =
    for ((leftAlias, col) <- onCols) yield JoinColumn(leftAlias, right.as, col)
}


