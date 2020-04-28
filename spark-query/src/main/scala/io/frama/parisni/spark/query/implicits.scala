package io.frama.parisni.spark.query

import java.sql.Timestamp

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit


package object implicits {
  import scala.language.implicitConversions

  implicit def columnPredicate(column: Column): Predicate = Predicate(column)

  implicit def timestampEvent(ts: Timestamp): Event = Event(lit(ts))
}

