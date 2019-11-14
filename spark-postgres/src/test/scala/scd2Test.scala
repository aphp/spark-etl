import java.sql.Timestamp

import io.frama.parisni.spark.dataframe.DFTool
import io.frama.parisni.spark.postgres.SparkSessionTestWrapper
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.junit.Test

class ScdTest extends QueryTest with SparkSessionTestWrapper {

  @Test
  def verifyScd2(): Unit = {
    import spark.implicits._

    (
      (1L, "a", "bob", new Timestamp(1), 1) ::
        (3L, "b", "bob", null, 2) ::
        Nil).toDF("id", "key", "cd", "end_date", "hash")
      .write
      .format("postgres")
      .option("url", getPgUrl)
      .option("table", "scd2table")
      .save

    val df = (
      (1L, "a", "jim") ::
        (2L, "b", "johm") ::
        Nil).toDF("id", "key", "cd")

    df.write.format("postgres")
      .option("url", getPgUrl)
      .option("type", "scd2")
      .option("joinKey", "key")
      .option("pk", "id")
      .option("endCol", "end_date")
      .option("table", "scd2table")
      .save

    val result = (
      (1L, "a", "bob") ::
        (1L, "a", "jim") ::
        (2L, "b", "johm") ::
        (3L, "b", "bob") ::
        Nil).toDF("id", "key", "cd")

    checkAnswer(spark.read.format("postgres")
      .option("url", getPgUrl)
      .option("query", "select * from scd2table")
      .load
      .select("id", "key", "cd")
      , result)

  }

  @Test
  def verifyScd2LowLevel(): Unit = {
    import spark.implicits._
    val table: String = "test_scd2_low"
    val joinKey: List[String] = "key1" :: "key2" :: Nil
    val endDatetimeCol: String = "end_date"
    val partitions: Option[Int] = Some(2)
    val multiline: Option[Boolean] = Some(false)
    val pk: String = "id"

    val df: DataFrame = ((1, "a", "b", "jim", new Timestamp(1L), -1) ::
      Nil).toDF("id", "key1", "key2", "cd", "end_date", "hash")
    getPgTool().tableCreate("test_scd2_low", df.schema)
    getPgTool().outputScd2Hash(table, DFTool.dfAddHash(df.drop("hash", "end_date")), pk, joinKey, endDatetimeCol, partitions, multiline)

    val df1: DataFrame = ((1, "a", "b", "bob", new Timestamp(1L), -1) ::
      Nil).toDF("id", "key1", "key2", "cd", "end_date", "hash")
    getPgTool().outputScd2Hash(table,DFTool.dfAddHash( df1.drop("hash", "end_date")), pk, joinKey, endDatetimeCol, partitions, multiline)
    // this should make nothing
    getPgTool().outputScd2Hash(table, DFTool.dfAddHash(df1.drop("hash", "end_date")), pk, joinKey, endDatetimeCol, partitions, multiline)

    val result = getPgTool().inputBulk("select * from test_scd2_low")

    val goal: DataFrame = (
      ("a", "b", "bob") ::
        ("a", "b", "jim") ::
        Nil).toDF("key1", "key2", "cd")

    checkAnswer(result.select("key1", "key2", "cd")
      , goal)

  }

}

