package io.frama.parisni.spark.query

class EventTest extends QueryBaseTest {

  test("truth table") {
    val df = spark.sql("SELECT 1 as `ok`")
    val one = DataFrameQuery(df, "one")
    assertQuery(1) {
      one | Event.now
    }
    assertQuery(0) {
      one | ((Event.now > Event.now) || (Event.now < Event.now))
    }
    assertQuery(1) {
      one | ((Event.now >= Event.now) && (Event.now <= Event.now))
    }
    assertQuery(1) {
      one | (Event.now.after(Event.now) && Event.now.before(Event.now))
    }
    assertQuery(1) {
      one | (Event.now > (Event.now - "1 minute"))
    }
    assertQuery(1) {
      one | (Event.now < (Event.now + "1 day"))
    }
  }

  test("filter: events") {
    assertQuery(messages + 1) {
      message | message.happens
    }
    assertQuery(messages + 1) {
      message | message.happens.before(Event.now)
    }
    assertQuery(messages + 1) {
      message | message.happens.before(Event.now - "1 day")
    }
  }

}
