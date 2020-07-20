package io.frama.parisni.spark.query

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class PredicateTest extends QueryBaseTest {

  test("truth table") {
    val t = TruePredicate
    val f = FalsePredicate
    val e = EmptyPredicate
    val foo = Predicate(lit("foo"))
    val bar = Predicate(lit("bar"))

    assert(t === !f)
    assert(f === !t)
    assert((!(!foo)) === foo)
    assert((e && foo) === foo)
    assert((e || foo) === foo)
    assert((t && foo) === foo)
    assert((t || foo) === t)
    assert((f && foo) === f)
    assert((f || foo) === foo)
    assert(!(foo && bar) === (!foo || !bar))
    assert(!(foo || bar) === (!foo && !bar))
  }

  test("filter: literals") {
    assertQuery(people) {
      person | TruePredicate
    }
    assertQuery(0) {
      person | FalsePredicate
    }
    // when left alone, empty doesn't filter
    assertQuery(people) {
      person | EmptyPredicate
    }
  }

  test("filter: equatable") {
    val person2 = new Query with Equatable[Person] {
      override def df: DataFrame = peopleDf
      override def as: String = "person"
      override def eq(a: Person): Predicate =
        Predicate(df("person_id") === a.person_id)
    }
    assertQuery(1) {
      person2 | (person2 === alice)
    }
    assertQuery(people - 1) {
      person2 | (person2 !== alice)
    }
  }

  test("filter: combinations") {
    val isAlice = Predicate(person("name") === alice.name)
    val isBob = Predicate(person("person_id") === bob.person_id)
    assertQuery(1) {
      person | isAlice
    }
    assertQuery(2) {
      person | (isAlice || isBob)
    }
    assertQuery(0) {
      person | (isAlice && isBob)
    }
    assertQuery(0) {
      person | isAlice | isBob
    }
    assertQuery(people - 2) {
      person | (!isAlice && !isBob)
    }
    assertQuery(people - 2) {
      person | !(isAlice || isBob)
    }
  }
}
