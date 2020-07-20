package io.frama.parisni.spark.query

import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.functions.{col, countDistinct}

class JoinTest extends QueryBaseTest {

  test("inner join") {
    // messages from people
    assertQuery(
      messages, {
        peopleDf.join(
          messagesDf,
          peopleDf("person_id") === messagesDf("person_id"),
          "inner"
        )
      }
    ) {
      person + message
    }
    assertResult(InnerJoin(person, message))(person + message)
  }

  test("left outer join") {
    // People and their messages, or not
    assertQuery(
      messages + 1, {
        peopleDf.join(
          messagesDf,
          peopleDf("person_id") === messagesDf("person_id"),
          "left_outer"
        )
      }
    ) {
      person % message
    }
    // Only Carlos didn't post any message
    assertQuery(1) {
      person % message | !message.happens
    }
    // Make sure messages with no author don't count
    assertQuery(0) {
      person % message | !person.happens
    }
  }

  test("right outer join") {
    // Messages and their authors, or not
    assertQuery(
      messages + 1, {
        peopleDf.join(
          messagesDf,
          peopleDf("person_id") === messagesDf("person_id"),
          "right_outer"
        )
      }
    ) {
      person %> message
    }
    // One message has no author
    assertQuery(1) {
      person %> message | !person.happens
    }
    // Make sure people with no messages don't count
    assertQuery(0) {
      person %> message | !message.happens
    }
  }

  test("full outer join") {
    // People and their messages, or not
    assertQuery(
      messages + 2, {
        peopleDf.join(
          messagesDf,
          peopleDf("person_id") === messagesDf("person_id"),
          "full_outer"
        )
      }
    ) {
      person %% message
    }
    // Only Carlos didn't post any message
    assertQuery(1) {
      person %% message | !message.happens
    }
    // One message has no author
    assertQuery(1) {
      person %% message | !person.happens
    }
  }

  test("left anti join") {
    // Only Carlos didn't post any message
    assertQuery(
      1, {
        peopleDf.join(
          messagesDf,
          peopleDf("person_id") === messagesDf("person_id"),
          "left_anti"
        )
      }
    ) {
      person - message
    }
  }

  test("left semi join") {
    // Only Carlos didn't post any message, all others did
    assertQuery(
      people - 1, {
        peopleDf.join(
          messagesDf,
          peopleDf("person_id") === messagesDf("person_id"),
          "left_semi"
        )
      }
    ) {
      person ^ message
    }
    // cannot use filter after, either filter right before or use .on() to filter on left columns
    assertThrows[AnalysisException]((person ^ message | message.happens).df)
  }

  test("cross join") {
    assertQuery(
      people * people, {
        peopleDf.crossJoin(peopleDf)
      }
    ) {
      person * person
    }
  }

  test("union query") {

    assertQuery(
      people * 2, {
        peopleDf.unionByName(peopleDf)
      }
    ) {
      person & person
    }
  }

  test("joiners") {
    // messages from people
    assertQuery(
      messages, {
        // auto-resolve...
        (person + message).df
        // (column order swapped)
          .select(
            messagesDf.columns.map(messagesDf(_)) ++ peopleDf.columns.map(
              peopleDf(_)
            ): _*
          )
      }
    ) {
      // ...should be symmetrical
      message + person
    }
    assertQuery(messages) {
      // common fields
      person + ~message
    }
    assertResult(InnerJoin(person, message, CommonColumnsJoiner))(
      person + ~message
    )
    assertQuery(messages) {
      // explicit column names
      person + message.on("person_id" /*, ... */ )
    }
    assertResult(InnerJoin(person, message, "person_id" /*, ... */ ))(
      person + message.on("person_id" /*, ... */ )
    )
    // topic has author_id, not person_id
    assertThrows[IllegalArgumentException](person + topic)
    assertThrows[IllegalArgumentException](person + topic.on("person_id"))
    assertThrows[IllegalArgumentException](
      person + topic.on(person -> "person_id")
    )
    assertThrows[IllegalArgumentException](
      person + topic.on("person_id" -> "person_id")
    )

    // messages from topic authors
    assertQuery(
      5, {
        peopleDf
          .join(
            topicsDf,
            peopleDf("person_id") === topicsDf("author_id"),
            "inner"
          )
          .join(
            messagesDf,
            (peopleDf("person_id") === messagesDf("person_id"))
              && (topicsDf("topic_id") === messagesDf("topic_id")),
            "inner"
          )
      }
    ) {
      // left col -> right col
      person + topic.on("person_id" -> "author_id") + message
    }
    assertQuery(
      5, {
        // auto-resolution should be symmetrical
        (topic + person.on("author_id" -> "person_id") + message).df
        // (column order swapped)
          .select(
            peopleDf.columns.map(peopleDf(_))
              ++ topicsDf.columns.map(topicsDf(_))
              ++ messagesDf.columns.map(messagesDf(_)): _*
          )
      }
    ) {
      // same, then use left query -> col
      (person
        + topic.on("person_id" -> "author_id")
        + message.on(person -> "person_id", topic -> "topic_id"))
    }
  }

  test("id fields") {
    val peopleDf2 = peopleDf.withColumnRenamed("person_id", "id")
    val person2 = Query(peopleDf2, "person")
    // messages from people should still auto-resolve
    assertQuery(messages) {
      person2 + message
    }

    val peopleDf3 = peopleDf.withColumnRenamed("person_id", "pid")
    val person3: Query = new Query {
      override def df: DataFrame = peopleDf3
      override val as: String = "p"
      override val joinAs: String = "person"
      override val idFields: Seq[String] = List("pid")
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
    assertQuery(
      2, {
        messagesDf
          .as("parent")
          .join(
            messagesDf.as("message"),
            col("parent.message_id") === col("message.parent_id"),
            "inner"
          )
      }
    ) {
      // explicit auto-qualified columns
      parent + message.on("message_id" -> "parent_id")
    }
    assertQuery(2) {
      // explicit leaves columns
      val reply = message.alias("reply")
      parent + reply.on(parent("message_id") === reply("parent_id")) | (parent(
        "message_id"
      ) =!= reply("message_id"))
    }
    assertQuery(2) {
      val parentAndReplies = parent + message.on("message_id" -> "parent_id")
      val JoinQuery(par, reply) = parentAndReplies
      parentAndReplies | (par("message_id") =!= reply("message_id"))
    }
  }

  test("select") {
    // Messages from topic authors
    // select with automatic prefix
    assertDF(
      5, {
        peopleDf
          .join(
            topicsDf,
            peopleDf("person_id") === topicsDf("author_id"),
            "inner"
          )
          .join(
            messagesDf,
            (peopleDf("person_id") === messagesDf("person_id"))
              && (topicsDf("topic_id") === messagesDf("topic_id")),
            "inner"
          )
          .select(
            peopleDf.columns.map(c => peopleDf(c).as("person_" + c))
              ++ messagesDf.columns.map(c =>
                messagesDf(c).as("message_" + c)
              ): _*
          )
      }
    ) {
      (person + topic.on("person_id" -> "author_id") + message)
        .select(person, message)
    }
    // select all with automatic prefix
    assertDF(
      messages, {
        peopleDf
          .join(
            messagesDf,
            peopleDf("person_id") === messagesDf("person_id"),
            "inner"
          )
          .select(
            peopleDf.columns.map(c => peopleDf(c).as("person_" + c))
              ++ messagesDf.columns
                .map(c => messagesDf(c).as("message_" + c)): _*
          )
      }
    ) {
      (person + message).select()
    }
    // select with manual prefix
    assertDF(
      messages, {
        peopleDf
          .join(
            messagesDf,
            peopleDf("person_id") === messagesDf("person_id"),
            "inner"
          )
          .select(
            peopleDf.columns.map(c => peopleDf(c).as("p_" + c))
              ++ messagesDf.columns.map(c => messagesDf(c).as("m_" + c)): _*
          )
      }
    ) {
      (person + message).select(person -> "p_", message -> "m_")
    }
    // select with no prefix, skipping duplicates
    assertDF(
      5, {
        peopleDf
          .join(
            topicsDf,
            peopleDf("person_id") === topicsDf("author_id"),
            "inner"
          )
          .join(
            messagesDf,
            (peopleDf("person_id") === messagesDf("person_id"))
              && (topicsDf("topic_id") === messagesDf("topic_id")),
            "inner"
          )
          .drop(messagesDf("person_id"))
          .drop(messagesDf("topic_id"))
      }
    ) {
      (person + topic.on("person_id" -> "author_id") + message)
        .select(skipDuplicates = true)
    }
  }

  test("group by") {
    assertDF(
      people - 1, {
        peopleDf
          .join(messagesDf, Seq("person_id"), "inner")
          .groupBy(peopleDf.columns.map(peopleDf(_)): _*)
          .agg(countDistinct(messagesDf("message_id")))
      }
    ) {
      // Carlos didn't post
      (person + message) / person
    }
    assertDF(
      people, {
        peopleDf
          .join(messagesDf, Seq("person_id"), "left_outer")
          .groupBy(peopleDf.columns.map(peopleDf(_)): _*)
          .agg(countDistinct(messagesDf("message_id")))
      }
    ) {
      (person % message) / person
    }
    assertDF(
      people + 1, {
        peopleDf
          .join(
            messagesDf,
            peopleDf("person_id") === messagesDf("person_id"),
            "full_outer"
          )
          .groupBy(peopleDf.columns.map(peopleDf(_)): _*)
          .agg(countDistinct(messagesDf("message_id")))
      }
    ) {
      // may still get one null on full-outer join
      (person %% message) / person
    }
    assertDF(
      people - 1, {
        peopleDf
          .join(messagesDf, Seq("person_id"), "inner")
          .join(topicsDf, Seq("topic_id"), "inner")
          .groupBy(peopleDf.columns.map(peopleDf(_)): _*)
          .agg(
            countDistinct(messagesDf("message_id")),
            countDistinct(messagesDf("topic_id"))
          )
      }
    ) {
      (person + message + topic) / person
    }
  }
}
