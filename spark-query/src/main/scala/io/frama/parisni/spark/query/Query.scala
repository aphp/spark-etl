package io.frama.parisni.spark.query

import org.apache.spark.sql.{Column, DataFrame}

import scala.reflect.runtime.universe.{TypeTag, typeOf}


trait Query {
  def df: DataFrame
  def as: String
  def joinAs: String = as
  def idField: String = "id"

  // Filter
  def |(c: Column): FilterQuery = FilterQuery(this, c, as)
  def |(p: Predicate): FilterQuery = |(p.toColumn)

  // Join by automatic column name from this.leaves' joinAs
  def +(q: Query): InnerJoin = InnerJoin(this, q)
  def -(q: Query): LeftAntiJoin = LeftAntiJoin(this, q)
  def %(q: Query): LeftOuterJoin = LeftOuterJoin(this, q)
  def *(q: Query): CrossJoin = CrossJoin(this, q)
  def &(q: Query): UnionQuery = UnionQuery(this, q)

  // Join by...
  def +(j: Joiner): InnerJoin = InnerJoin(this, j)
  def -(j: Joiner): LeftAntiJoin = LeftAntiJoin(this, j)
  // ...by columns in common (name & type)
  def unary_~ : CommonColumnsJoiner = CommonColumnsJoiner(this)
  // ...by explicit Column
  def on(column: Column): ColumnJoiner = ColumnJoiner(this, column)
  // ...by explicit column name(s)
  def on[A : TypeTag](onCols: (A, String) *): Joiner = onCols match {
    case ons: Seq[(String, String)] if typeOf[A] <:< typeOf[String] => ColumnNamesJoiner(this, ons :_*)
    case ons: Seq[(Query,  String)] if typeOf[A] <:< typeOf[Query]  =>
      AliasAndColumnNameJoiner(this, ons.map(on => (on._1.joinAs, on._2)) :_*)
  }
  def on(columnNames: String *): ColumnNameJoiner = ColumnNameJoiner(this, columnNames :_*)

  // Column selection
  protected def qualifiedColumns(qs: Query *): Seq[(String, String, Column)] =
    for {
      query <- if (qs.isEmpty) leaves else qs
      column <- query.df.columns
    } yield (query.as, column, query.df(column))

  def select(qs: (Query, String) *): DataFrame =
    df.select((for {
      (as, name, column) <- qualifiedColumns(qs.map(_._1) :_*)
      prefix = qs.find(_._1.as == as).get._2
      col = column.as(if (prefix.isEmpty) name else prefix + name)
    } yield col) :_*)

  def select(q1: Query, qs: Query *): DataFrame = select((q1 +: qs).map(q => (q, q.as + "_")) :_*)

  def select(skipDuplicates: Boolean, qs: Query *): DataFrame = {
    val cols = collection.mutable.Set.empty[String]
    df.select((for {
      (_, name, column) <- qualifiedColumns(qs :_*) if !skipDuplicates || cols.add(name)
    } yield column.as(name)) :_*)
  }

  def alias(as: String): AliasQuery = AliasQuery(as)(this)

  // Traversal
  def leaves: Seq[Query] = List(this)
  def apply(column: String): Column = df(column)
  override def toString: String = as
}

object Query {
  def apply(df: DataFrame, as: String): Query = DataFrameQuery(df, as)
  def apply(df: DataFrame, as: String, joinAs: String): Query = DataFrameQuery(df, as, joinAs)
}


trait QueryDecorator extends Query {
  val base: Query
  override def leaves: Seq[Query] = base.leaves
  override def apply(column: String): Column = base.apply(column)
}
