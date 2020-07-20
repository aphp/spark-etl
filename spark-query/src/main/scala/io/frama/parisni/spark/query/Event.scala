package io.frama.parisni.spark.query

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{current_timestamp, expr}

trait Event extends Predicate {
  def timestamp: Column

  def <=(e: Event = Event.now): Predicate = Predicate(timestamp <= e.timestamp)
  def >=(e: Event = Event.now): Predicate = Predicate(timestamp >= e.timestamp)
  def <(e: Event = Event.now): Predicate = Predicate(timestamp < e.timestamp)
  def >(e: Event = Event.now): Predicate = Predicate(timestamp > e.timestamp)
  def before(e: Event = Event.now): Predicate = this <= e
  def after(e: Event = Event.now): Predicate = this >= e

  def +(interval: String): Event =
    Event(timestamp.plus(expr(s"INTERVAL $interval")))
  def -(interval: String): Event =
    Event(timestamp.minus(expr(s"INTERVAL $interval")))

  override def toColumn: Column = timestamp.isNotNull
  override def toNotColumn: Option[Column] = Some(timestamp.isNull)
}
object Event {
  def apply(col: => Column): Event =
    new Event {
      override def timestamp: Column = col
    }
  val now: Event = apply(current_timestamp())
}

trait HasEvent {
  def event: Event
  def happens: Event = event
  def starts: Event = happens
  def ends: Event = happens
}
