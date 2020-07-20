package io.frama.parisni.spark.query

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, QueryTest, SparkSession}

class QueryBaseTest extends QueryTest {
  protected lazy val spark: SparkSession = QueryBaseTest.spark

  protected lazy val printExplanations: Boolean = false

  protected def assertQuery(count: Long): (=> Query) => Unit =
    assertQuery(None, Some(count))
  protected def assertQuery(
      count: Long,
      expected: => DataFrame
  ): (=> Query) => Unit =
    assertQuery(Some(expected), Some(count))
  protected def assertQuery(expected: => DataFrame): (=> Query) => Unit =
    assertQuery(Some(expected), None)
  protected def assertQuery(
      expected: => Option[DataFrame],
      count: Option[Long]
  )(q: => Query) {
    println(q)
    println(q.treeString)
    assertDF(count, expected)(q.df)
  }

  protected def assertDF(count: Long)(df: => DataFrame) {
    assertDF(Some(count), None)(df)
  }
  protected def assertDF(expected: => DataFrame)(df: => DataFrame) {
    assertDF(None, Some(expected))(df)
  }
  protected def assertDF(count: Long, expected: => DataFrame)(
      df: => DataFrame
  ) {
    assertDF(Some(count), Some(expected))(df)
  }
  protected def assertDF(count: Option[Long], expected: => Option[DataFrame])(
      df: => DataFrame
  ) {
    if (printExplanations) {
      df.explain(false)
      df.printSchema()
    }
    df.show()
    count.foreach(c => assertResult(c)(df.count()))
    expected.foreach(checkAnswer(df, _))
  }

  import QueryBaseTest.string2Timestamp

  lazy val peopleDf: DataFrame = spark.createDataFrame(
    Person(1, "Alice", "2000-01-01 12:00:00")
      :: Person(2, "Bob", "2000-02-01 12:00:00")
      :: Person(3, "Carlos", "2000-03-01 12:00:00")
      :: Person(4, "Dave", "2000-04-01 12:00:00")
      :: Person(5, "Eve", "2000-05-01 12:00:00")
      :: Nil
  )
  lazy val people: Long = peopleDf.count()
  lazy val Array(alice, bob, carlos) = peopleDf
    .take(3)
    .map(r => Person(r.getShort(0), r.getString(1), r.getTimestamp(2)))

  lazy val topicsDf: DataFrame = spark.createDataFrame(
    Topic(10, 1, "It's my birthday", "2020-01-01 12:00:00")
      :: Topic(20, 1, "Who let the dogs out?", "2020-01-02 12:00:00")
      :: Topic(30, 2, "My turn", "2020-02-01 12:00:00")
      :: Nil
  )
  lazy val topics: Long = topicsDf.count()

  lazy val messagesDf: DataFrame = spark.createDataFrame(
    Message(100, 1, 10, "Come celebrate!", "2020-01-01 12:00:00")
      :: Message(110, 2, 10, "What time?", "2020-01-01 13:00:00")
      :: Message(111, 1, 10, "8p", "2020-01-01 14:00:00", Some(110))
      :: Message(112, 4, 10, "Can do 9p", "2020-01-01 15:00:00", Some(111))
      :: Message(120, 5, 10, "Yay, party!", "2020-01-01 16:00:00")
      :: Message(200, 1, 20, "I can't find Cheddar", "2020-01-02 12:00:00")
      :: Message(201, 5, 20, "try the backyard?", "2020-01-02 13:00:00")
      :: Message(202, 1, 20, "nvm found him", "2020-01-02 14:00:00")
      :: Message(300, 2, 30, "This time Dave's DJ", "2020-02-01 12:00:00")
      :: Message(404, 0, 0, "No author nor topic", "2020-01-15 12:00:00")
      :: Nil
  )
  lazy val allMessages: Long = messagesDf.count()
  // only with topic/author
  lazy val messages: Long = messagesDf
    .where(messagesDf("person_id") > 0 && messagesDf("topic_id") > 0)
    .count()

  lazy val person: DataFrameEventQuery =
    DataFrameEventQuery(peopleDf, "person", "born")
  lazy val topic: DataFrameEventQuery =
    DataFrameEventQuery(topicsDf, "topic", "created")
  lazy val message: DataFrameEventQuery =
    DataFrameEventQuery(messagesDf, "message", "posted")

}

object QueryBaseTest {
  lazy val spark: SparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName("spark test session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

  implicit def string2Timestamp(ts: String): Timestamp = Timestamp.valueOf(ts)
}

case class Person(person_id: Short, name: String, born: Timestamp)
case class Topic(
    topic_id: Short,
    author_id: Short,
    title: String,
    created: Timestamp
)
case class Message(
    message_id: Int,
    person_id: Short,
    topic_id: Short,
    text: String,
    posted: Timestamp,
    parent_id: Option[Int] = None
)
