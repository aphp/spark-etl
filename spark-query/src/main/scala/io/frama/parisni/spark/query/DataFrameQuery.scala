package io.frama.parisni.spark.query

import org.apache.spark.sql.DataFrame


case class DataFrameQuery(df: DataFrame, as: String, override val joinAs: String,
                          override val idFields: Seq[String] = List("id")) extends Query

object DataFrameQuery {
  def apply(df: DataFrame, as: String): DataFrameQuery = DataFrameQuery(df, as, as)
}


case class DataFrameEventQuery(df: DataFrame, as: String,
                               eventCol: String, startsCol: String, endsCol: String,
                               override val joinAs: String,
                               override val idFields: Seq[String] = List("id")) extends Query with HasEvent {
  override def event: Event = Event(df(eventCol))
  override def starts: Event = Event(df(startsCol))
  override def ends: Event = Event(df(endsCol))
}

object DataFrameEventQuery {
  def apply(df: DataFrame, as: String, eventCol: String): DataFrameEventQuery =
    DataFrameEventQuery(df, as, eventCol, eventCol, eventCol)
  def apply(df: DataFrame, as: String, eventCol: String, startsCol: String, endsCol: String): DataFrameEventQuery =
    DataFrameEventQuery(df, as, eventCol, startsCol, endsCol, as)
}

