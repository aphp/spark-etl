package io.frama.parisni.spark.query

trait Equatable[A] {
  def eq(a: A): Predicate
  def ===(a: A): Predicate = eq(a)
  def neq(a: A): Predicate = !eq(a)
  def !==(a: A): Predicate = neq(a)
  def =!=(a: A): Predicate = neq(a)
}
