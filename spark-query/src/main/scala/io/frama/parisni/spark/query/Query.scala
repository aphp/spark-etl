package io.frama.parisni.spark.query

import org.apache.spark.sql.functions.{count, countDistinct, first}
import org.apache.spark.sql.{Column, DataFrame}

import scala.reflect.runtime.universe.{TypeTag, typeOf}

trait Query {
  def df: DataFrame
  def as: String
  def joinAs: String = as
  def idFields: Seq[String] = List("id")

  // Filter
  def |(c: Column): FilterQuery = FilterQuery(this, c, as)
  def |(p: Predicate): FilterQuery = |(p.toColumn)

  // Join by automatic column name from this.leaves' joinAs
  def +(q: Query): InnerJoin = InnerJoin(this, q)
  def -(q: Query): LeftAntiJoin = LeftAntiJoin(this, q)
  def %(q: Query): LeftOuterJoin = LeftOuterJoin(this, q)
  def %>(q: Query): RightOuterJoin = RightOuterJoin(this, q)
  def %%(q: Query): FullOuterJoin = FullOuterJoin(this, q)
  def ^(q: Query): LeftSemiJoin = LeftSemiJoin(this, q)
  def *(q: Query): CrossJoin = CrossJoin(this, q)
  def &(q: Query): UnionQuery = UnionQuery(this, q)

  // Join by...
  def +(j: Joiner): InnerJoin = InnerJoin(this, j)
  def -(j: Joiner): LeftAntiJoin = LeftAntiJoin(this, j)
  def %(j: Joiner): LeftOuterJoin = LeftOuterJoin(this, j)
  def %>(j: Joiner): RightOuterJoin = RightOuterJoin(this, j)
  def %%(j: Joiner): FullOuterJoin = FullOuterJoin(this, j)
  def ^(j: Joiner): LeftSemiJoin = LeftSemiJoin(this, j)
  // ...by columns in common (name & type)
  def unary_~ : CommonColumnsJoiner = CommonColumnsJoiner(this)
  // ...by explicit Column
  def on(column: Column): ColumnJoiner = ColumnJoiner(this, column)
  // ...by explicit column name(s)
  def on[A: TypeTag](onCols: (A, String)*): Joiner =
    onCols match {
      case ons: Seq[(String, String)] if typeOf[A] <:< typeOf[String] =>
        ColumnNamesJoiner(this, ons: _*)
      case ons: Seq[(Query, String)] if typeOf[A] <:< typeOf[Query] =>
        AliasAndColumnNameJoiner(this, ons.map(on => (on._1.as, on._2)): _*)
    }
  def on(columnNames: String*): ColumnNameJoiner =
    ColumnNameJoiner(this, columnNames: _*)

  // Column selection
  def select(q: (Query, String), qs: (Query, String)*): DataFrame =
    df.select((for {
      (query, prefix) <- q +: qs
      name <- query.df.columns
      column = query.df(name)
      col = column.as(if (prefix.isEmpty) name else prefix + name)
    } yield col): _*)

  def select(qs: Query*): DataFrame = {
    val Seq(head, tail @ _*) = if (qs.isEmpty) leaves else qs
    select((head, head.as + "_"), tail.map(q => (q, q.as + "_")): _*)
  }

  def select(skipDuplicates: Boolean, qs: Query*): DataFrame = {
    val cols = collection.mutable.Set.empty[String]
    df.select((for {
      query <- if (qs.isEmpty) leaves else qs
      name <- query.df.columns if !skipDuplicates || cols.add(name)
    } yield query.df(name)): _*)
  }

  // Group by (query), output its columns + agg as {ungroupedQuery.as} for other leaves
  def groupBy(
      qs: Seq[(Query, Seq[String])],
      agg: Query => Column = Query.autoCountDistinct
  ): DataFrame = {
    // TODO replace leaves by node traversal, could group on any nodes, as long as one isn't the ancestor of another
    require(qs.nonEmpty, "Nothing to group on")
    val group = for {
      (q, cols) <- qs
      _ = require(leaves.contains(q), "Cannot group by non-leaf " + q.as)
      _ = require(cols.nonEmpty, "Nothing to group on for " + q.as)
      col <- cols
    } yield q(col)
    val retrieve = for {
      (q, cols) <- qs
      col <- q.df.columns if !cols.contains(col)
    } yield first(q(col)).as(col)
    val aggregate =
      for { q <- leaves if !qs.map(_._1).contains(q) } yield agg(q)
    val Seq(firstAgg, tailAgg @ _*) = retrieve ++ aggregate
    df.groupBy(group: _*).agg(firstAgg, tailAgg: _*)
  }
  def groupBy(qs: Query*): DataFrame =
    groupBy(
      qs.map(q =>
        (
          q,
          if (q.idFields.forall(q.df.columns.contains)) q.idFields
          else q.df.columns.toSeq
        )
      )
    )
  def /(qs: Query*): DataFrame = groupBy(qs: _*)

  def alias(as: String): AliasQuery = AliasQuery(as)(this)

  // Traversal
  def leaves: Seq[Query] = List(this)
  def nodes: Seq[Query] = Seq.empty

  def apply(column: String): Column = df(column)

  override def toString: String = as

  def nodeString: String = s"[${getClass.getSimpleName}] $as"
  def treeString: String = {
    val builder = new StringBuilder
    Query.treeString(this, builder)
    builder.mkString
  }
}

object Query {
  def apply(df: DataFrame, as: String): Query = DataFrameQuery(df, as)
  def apply(df: DataFrame, as: String, joinAs: String): Query =
    DataFrameQuery(df, as, joinAs)

  def unapplySeq(q: Query): Option[Seq[Query]] = Some(q.nodes)

  def treeString(q: Query, builder: StringBuilder, prefix: String = "") {
    if (builder.nonEmpty) {
      builder.append('\n')
    }
    builder.append(prefix).append(q.nodeString)
    q.nodes.foreach(
      treeString(_, builder, if (prefix.isEmpty) "  |- " else s"  |  $prefix")
    )
  }

  def autoCountDistinct(q: Query): Column =
    if (q.idFields.forall(q.df.columns.contains)) countDistinctIds(q)
    else countDistinctNonNullableColumns(q)
  def countDistinctNonNullableColumns(q: Query): Column = {
    val cols = q.df.schema.filterNot(_.nullable).map(f => q(f.name))
    require(
      cols.nonEmpty,
      "Count distinct needs non-nullable columns, none found"
    )
    countDistinct(cols.head, cols.tail: _*).as(q.as)
  }
  def countDistinctIds(q: Query): Column =
    countDistinct(q.df(q.idFields.head), q.idFields.tail.map(q.df(_)): _*)
      .as(q.as)
  def countAll(q: Query): Column =
    count(q.idFields.find(q.df.columns.contains).getOrElse(q.df.columns.head))
      .as(q.as)
}
