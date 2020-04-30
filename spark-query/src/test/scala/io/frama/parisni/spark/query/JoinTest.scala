package io.frama.parisni.spark.query

import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.functions.col

class JoinTest extends QueryBaseTest {

  test("inner join") {
    // messages from people
    assertQuery(messages) {
      person + message
    }
    assertQuery(messages) {
      InnerJoin(person, message)
    }
  }

  test("left outer join") {
    // People and their messages, or not
    assertQuery(messages + 1) {
      person % message
    }
    // Only Carlos didn't post any message
    assertQuery(1) {
      person % message | ! message.happens
    }
  }

  test("left anti join") {
    // Only Carlos didn't post any message
    assertQuery(1) {
      person - message
    }
  }

  test("left semi join") {
    // Only Carlos didn't post any message, all others did
    assertQuery(people - 1) {
      person ^ message
    }
    // cannot use filter after, either filter right before or use .on() to filter on left columns
    assertThrows[AnalysisException]((person ^ message | message.happens).df)
    // right is not returned
    assertDF(people - 1, _.sameElements(peopleDf.columns)) {
      (person ^ message).df
    }
    // same goes for select
    assertDF(people - 1, _.sameElements(peopleDf.columns.map("person_" + _))) {
      (person ^ message).select()
    }
  }

  test("cross join") {
    assertQuery(people * people) {
      person * person
    }
  }

  test("union query") {
    assertQuery(people * 2) {
      person & person
    }
  }

  test("joiners") {
    // messages from people
    assertQuery(messages) {
      // auto-resolve
      person + message
    }
    assertQuery(messages) {
      // common fields
      person + ~message
    }
    assertQuery(messages) {
      // common fields
      InnerJoin(person, message, CommonColumnsJoiner)
    }
    assertQuery(messages) {
      // explicit column names
      person + message.on("person_id" /*, ... */)
    }
    assertQuery(messages) {
      // explicit column names
      InnerJoin(person, message, "person_id" /*, ... */)
    }
    // topic has author_id, not person_id
    assertThrows[IllegalArgumentException](person + topic)
    assertThrows[IllegalArgumentException](person + topic.on("person_id"))
    assertThrows[IllegalArgumentException](person + topic.on(person -> "person_id"))
    assertThrows[IllegalArgumentException](person + topic.on("person_id" -> "person_id"))

    // messages from topic authors
    assertQuery(5) {
      // left col -> right col
      person + topic.on("person_id" -> "author_id") + message
    }
    assertQuery(5) {
      // same, then left query -> col
      (person
        + topic.on("person_id" -> "author_id")
        + message.on(person -> "person_id", topic -> "topic_id"))
    }
  }

  test("id field") {
    val peopleDf2 = peopleDf.withColumnRenamed("person_id", "id")
    val person2: Query = new Query {
      override def df: DataFrame = peopleDf2
      override val as: String = "person"
    }
    // messages from people should still auto-resolve
    assertQuery(messages) {
      person2 + message
    }

    val peopleDf3 = peopleDf.withColumnRenamed("person_id", "pid")
    val person3: Query = new Query {
      override def df: DataFrame = peopleDf3
      override val as: String = "p"
      override val joinAs: String = "person"
      override val idField: String = "pid"
    }
    // messages from people should still auto-resolve
    assertQuery(messages) {
      person3 + message
    }
  }

  test("self join") {
    // replies only, with their parent
    val parent = message.alias("parent")
    assertResult("parent.message_id") {
      parent.df("message_id").expr.asInstanceOf[NamedExpression].qualifiedName
    }
    assertResult("parent.message_id") {
      parent("message_id").toString
    }
    assertQuery(2) {
      // explicit qualified columns
      parent + message.on(col("parent.message_id") === col("message.parent_id"))
    }
    assertQuery(2) {
      // explicit leaves columns
      val reply = message.alias("reply")
      parent + reply.on(parent("message_id") === reply("parent_id"))
    }
    assertQuery(2) {
      // explicit auto-qualified columns
      parent + message.on("message_id" -> "parent_id")
    }
    assertQuery(2) {
      // explicit leaves columns
      val reply = message.alias("reply")
      parent + reply.on(parent("message_id") === reply("parent_id")) | (parent("message_id") !== reply("message_id"))
    }
    assertQuery(2) {
      val parentAndReplies = parent + message.on("message_id" -> "parent_id")
      val JoinQuery(par, reply) = parentAndReplies
      parentAndReplies | (par("message_id") !== reply("message_id"))
    }
  }

test("select") {
    // Messages from topic authors
    // select with automatic prefix
    assertDF(5, _.sameElements(peopleDf.columns.map("person_" + _) ++ messagesDf.columns.map("message_" + _))) {
      (person + topic.on("person_id" -> "author_id") + message).select(person, message)
    }
    // select all with automatic prefix
    assertDF(messages, _.sameElements(peopleDf.columns.map("person_" + _) ++ messagesDf.columns.map("message_" + _))) {
      (person + message).select()
    }
    // select with manual prefix
    assertDF(messages, _.sameElements(peopleDf.columns.map("p_" + _) ++ messagesDf.columns.map("m_" + _))) {
      (person + message).select(person -> "p_", message -> "m_")
    }
    // select with no prefix, skipping duplicates
    assertDF(5, _.sorted.sameElements(Set(peopleDf, topicsDf, messagesDf).flatMap(_.columns).toSeq.sorted)) {
      (person + topic.on("person_id" -> "author_id") + message).select(skipDuplicates = true)
    }
  }
}
