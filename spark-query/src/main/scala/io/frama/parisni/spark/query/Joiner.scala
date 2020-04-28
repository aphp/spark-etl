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
    val cols = for {
      j <- resolveColumns(left)
      lq <- left.leaves.find(_.as == j.leftAlias)
      lcol = (if (lq == left) aliasedLeft else lq)(j.leftColumn)
      rcol = aliasedRight(j.rightColumn)
    } yield lcol === rcol
    cols match {
      case Nil => throw new IllegalArgumentException(
        s"No column found to join ${left.as} <-> ${right.as}, strategy=${getClass.getSimpleName}")
      case _   => ResolvedJoin(aliasedLeft, aliasedRight, cols.reduce(_ && _))
    }
  }
}


case class JoinColumn(leftAlias: String, leftColumn: String, rightColumn: String)

object JoinColumn {
  def apply(leftAlias: String, column: String): JoinColumn = new JoinColumn(leftAlias, column, column)
}


// left + right
// Guess column(s) to join on, up to 1 per left leave, based on leaves' joinAs:
// on the left: id, {joinAs}_id, {as}_id
// on the right: {left.joinAs}_id, {left.joinAs}_fk, {left.joinAs}
case class AutoColumnsJoiner(right: Query) extends NameJoiner {
  override def resolveColumns(left: Query): Seq[JoinColumn] = AutoColumnsJoiner.guessJoinColumns(left, right.df.columns)
}

object AutoColumnsJoiner {
  val guesses: Seq[String => String] = Seq(_ + "_id", _ + "_fk", s => s)
  def guessJoinColumns(left: Query, rightColumns: Array[String]): Seq[JoinColumn] =
    for {
      left <- left.leaves
      rightCol <- guesses.map(_(left.joinAs)).find(rightColumns.contains)
      leftCol <- Seq(left.idField, left.joinAs + "_id", left.as + "_id").find(left.df.columns.contains)
    } yield JoinColumn(left.as, leftCol, rightCol)
}


// left + ~right
// joins on common columns in schema
case class CommonColumnsJoiner(right: Query) extends NameJoiner {
  override def resolveColumns(left: Query): Seq[JoinColumn] = {
    val cols = collection.mutable.Set.empty[StructField]
    for {
      l <- left.leaves
      col <- l.df.schema if right.df.schema.contains(col) && cols.add(col)
    } yield JoinColumn(l.as, col.name)
  }
}


// left + right.on("col", ...)
case class ColumnNameJoiner(right: Query, onCols: String *) extends NameJoiner {
  require(onCols.forall(right.df.columns.contains), "Some join columns not found in right query")

  override def resolveColumns(left: Query): Seq[JoinColumn] =
    onCols.map(col => JoinColumn(left.leaves.filter(_.df.columns.contains(col)).head.as, col))
}


// left + right.on("leftCol" -> "rightCol", ...)
case class ColumnNamesJoiner(right: Query, onCols: (String, String) *) extends NameJoiner {
  require(onCols.forall(on => right.df.columns.contains(on._2)), "Some join columns not found in right query")

  override def resolveColumns(left: Query): Seq[JoinColumn] =
    onCols.map(cols => JoinColumn(left.leaves.filter(_.df.columns.contains(cols._1)).head.as, cols._1, cols._2))
}


// left + right.on(left -> "col", ...)
case class AliasAndColumnNameJoiner(right: Query, onCols: (String, String) *) extends NameJoiner {
  require(onCols.forall(on => right.df.columns.contains(on._2)), "Some join columns not found in right query")

  override def resolveColumns(left: Query): Seq[JoinColumn] = onCols.map(on => JoinColumn(on._1, on._2))
}


