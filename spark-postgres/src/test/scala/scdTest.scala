import io.frama.parisni.spark.postgres.SparkSessionTestWrapper
import org.apache.spark.sql.QueryTest
import org.junit.{Before, Test}

class ScdTest extends QueryTest with SparkSessionTestWrapper {

  @Before
  def initDatabase(): Unit = {
    import spark.implicits._
    val df = ((1L, "bob", 1) :: (3L, "bob", 2) :: Nil).toDF("id", "cd", "hash")

    df.write
      .format("postgres")
      .option("url", getPgUrl)
      .option("table", "scd1table")
      .save

  }

  @Test
  def verifyScd1(): Unit = {

    import spark.implicits._
    val df = ((1L, "jim") :: (2L, "johm") :: Nil).toDF("id", "cd")
    df.write.format("postgres")
      .option("url", getPgUrl)
      .option("type", "scd1")
      .option("joinKey", "id")
      .option("table", "scd1table")
      .save

    val result = ((1L, "jim") :: (2L, "johm") :: (3L, "bob") :: Nil).toDF("id", "cd")

    checkAnswer(spark.read.format("postgres")
      .option("url", getPgUrl)
      .option("query", "select * from scd1table")
      .load
      .select("id", "cd")
      , result
    )

  }

}

