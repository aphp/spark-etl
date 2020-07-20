package io.frama.parisni.spark.postgres

import java.sql.Timestamp

import io.frama.parisni.spark.dataframe.DFTool
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.junit.Test

class Scd1Test extends QueryTest with SparkSessionTestWrapper {

  @Test
  def verifyScd1Csv(): Unit = verifyScd1("csv")

  def verifyScd1(bulkLoadMode: String): Unit = {
    import spark.implicits._

    ((1L, "a", "bob", new Timestamp(1), 1) ::
      (3L, "b", "bob", null, 2) ::
      Nil)
      .toDF("id", "key", "cd", "end_date", "hash")
      .write
      .format("io.frama.parisni.spark.postgres")
      .option("url", getPgUrl)
      .option("table", "scd1table")
      .option("bulkLoadMode", bulkLoadMode)
      .save

    val df = ((1L, "a", "jim") ::
      (2L, "b", "johm") ::
      Nil).toDF("id", "key", "cd")

    df.write
      .format("io.frama.parisni.spark.postgres")
      .option("url", getPgUrl)
      .option("type", "scd1")
      .option("joinKey", "key")
      .option("table", "scd1table")
      .save

    val result = ((1L, "a", "jim") ::
      (2L, "b", "johm") ::
      Nil).toDF("id", "key", "cd")

    checkAnswer(
      spark.read
        .format("io.frama.parisni.spark.postgres")
        .option("url", getPgUrl)
        .option("query", "select * from scd1table")
        .load
        .select("id", "key", "cd"),
      result
    )

  }

  @Test
  def verifyScd1Stream(): Unit = verifyScd1("stream")

  @Test
  def verifyScd1LowLevelCSV(): Unit = verifyScd1LowLevel(CSV)

  @Test
  def verifyScd1LowLevelStream(): Unit = verifyScd1LowLevel(Stream)

  def verifyScd1LowLevel(bulkLoadMode: BulkLoadMode): Unit = {
    import spark.implicits._

    val table: String = "test_scd1_low"
    val joinKey: List[String] = "key1" :: "key2" :: Nil

    val df: DataFrame = ((1, "a", "b", "jim", new Timestamp(1L), -1) ::
      Nil).toDF("id", "key1", "key2", "cd", "end_date", "hash")
    getPgTool(bulkLoadMode).tableCreate("test_scd1_low", df.schema)
    getPgTool(bulkLoadMode).outputScd1Hash(
      table,
      joinKey,
      DFTool.dfAddHash(df.drop("end_date", "hash"))
    )

    val df1: DataFrame = ((1, "a", "b", "bob", new Timestamp(1L), -1) ::
      Nil).toDF("id", "key1", "key2", "cd", "end_date", "hash")
    getPgTool(bulkLoadMode).outputScd1Hash(
      table,
      joinKey,
      DFTool.dfAddHash(df1.drop("end_date", "hash"))
    )
    // this should make nothing
    getPgTool(bulkLoadMode).outputScd1Hash(
      table,
      joinKey,
      DFTool.dfAddHash(df1.drop("end_date", "hash"))
    )

    val result = getPgTool().inputBulk("select * from test_scd1_low")

    val goal: DataFrame = (("a", "b", "bob") ::
      Nil).toDF("key1", "key2", "cd")

    checkAnswer(
      result.select("key1", "key2", "cd"),
      goal
    )
  }

  @Test
  def verifyScd1LowLevelPgBinaryStream(): Unit =
    verifyScd1LowLevel(PgBinaryStream)
  @Test
  def verifyScd1LowLevelPgBinaryFiles(): Unit =
    verifyScd1LowLevel(PgBinaryFiles)

  @Test
  def verifyScd1WithFilterCSV(): Unit = verifyScd1WithFilter("csv")

  @Test
  def verifyScd1WithFilterStream(): Unit = verifyScd1WithFilter("stream")

  @Test
  def verifyScd1WithFilterPgBinaryStream(): Unit =
    verifyScd1WithFilter("PgBinaryStream")

  def verifyScd1WithFilter(bulkLoadMode: String): Unit = {
    import spark.implicits._

    ((1L, "a", "bob", new Timestamp(1), 1) ::
      (3L, "b", "bob", null, 2) ::
      Nil)
      .toDF("id", "key", "cd", "end_date", "hash")
      .write
      .format("io.frama.parisni.spark.postgres")
      .option("url", getPgUrl)
      .option("table", "scd1table")
      .option("bulkLoadMode", bulkLoadMode)
      .save

    val df = ((1L, "a", "jim") ::
      (2L, "b", "johm") ::
      Nil).toDF("id", "key", "cd")

    df.write
      .format("io.frama.parisni.spark.postgres")
      .option("url", getPgUrl)
      .option("type", "scd1")
      .option("filter", "key is not null")
      .option("joinKey", "key")
      .option("table", "scd1table")
      .option("bulkLoadMode", bulkLoadMode)
      .save

    val result = ((1L, "a", "jim") ::
      (2L, "b", "johm") ::
      Nil).toDF("id", "key", "cd")

    checkAnswer(
      spark.read
        .format("io.frama.parisni.spark.postgres")
        .option("url", getPgUrl)
        .option("query", "select * from scd1table")
        .load
        .select("id", "key", "cd"),
      result
    )

  }

  @Test
  def verifyScd1WithFilterPgBinaryFiles(): Unit =
    verifyScd1WithFilterAndDelete("PgBinaryFiles")

  def verifyScd1WithFilterAndDelete(bulkLoadMode: String): Unit = {
    import spark.implicits._

    ((1L, "a", "bob", true, 1) ::
      (3L, "b", "bob", true, 2) ::
      Nil)
      .toDF("id", "key", "cd", "is_active", "hash")
      .write
      .format("io.frama.parisni.spark.postgres")
      .option("url", getPgUrl)
      .option("table", "scd1table")
      .option("bulkLoadMode", bulkLoadMode)
      .save

    val df = ((1L, "a", "jim") ::
      Nil).toDF("id", "key", "cd")

    df.write
      .format("io.frama.parisni.spark.postgres")
      .option("url", getPgUrl)
      .option("type", "scd1")
      .option("filter", "key is not null")
      .option("deleteSet", "is_active = false")
      .option("joinKey", "key")
      .option("table", "scd1table")
      .option("bulkLoadMode", bulkLoadMode)
      .save

    val result = ((1L, "a", "jim", true) ::
      (3L, "b", "bob", false) ::
      Nil).toDF("id", "key", "cd", "is_active")

    checkAnswer(
      spark.read
        .format("io.frama.parisni.spark.postgres")
        .option("url", getPgUrl)
        .option("query", "select * from scd1table")
        .load
        .select("id", "key", "cd", "is_active"),
      result
    )

  }

  @Test
  def verifyScd1WithFilterAndDeleteCSV(): Unit =
    verifyScd1WithFilterAndDelete("csv")
  @Test
  def verifyScd1WithFilterAndDeleteStream(): Unit =
    verifyScd1WithFilterAndDelete("stream")
  @Test
  def verifyScd1WithFilterAndDeletePgBinaryStream(): Unit =
    verifyScd1WithFilterAndDelete("PgBinaryStream")
  @Test
  def verifyScd1WithFilterAndDeletePgBinaryFiles(): Unit =
    verifyScd1WithFilterAndDelete("PgBinaryFiles")

  @Test
  def verifyScd1WithLongKey(): Unit = {
    import spark.implicits._

    ((1L, "a", "bob", true, 1) ::
      (3L, "b", "bob", true, 2) ::
      Nil)
      .toDF("id", "key", "cd", "is_active", "hash")
      .write
      .format("io.frama.parisni.spark.postgres")
      .option("url", getPgUrl)
      .option("table", "scd1tablelong")
      .save

    val df = ((1L, "a", "jim") ::
      Nil).toDF("id", "key", "cd")

    df.write
      .format("io.frama.parisni.spark.postgres")
      .option("url", getPgUrl)
      .option("type", "scd1")
      .option("joinKey", "id")
      .option("partitions", 4)
      .option("numSplits", 40)
      .option("table", "scd1tablelong")
      .save

    val result = ((1L, "a", "jim", true) ::
      (3L, "b", "bob", true) ::
      Nil).toDF("id", "key", "cd", "is_active")

    checkAnswer(
      spark.read
        .format("io.frama.parisni.spark.postgres")
        .option("url", getPgUrl)
        .option("query", "select * from scd1tablelong")
        .load
        .select("id", "key", "cd", "is_active"),
      result
    )
  }

  @Test
  def verifyScd1WithStringKey(): Unit = {
    import spark.implicits._

    ((1L, "abc_123", "bob", true, 1) ::
      (3L, "abc_321", "bob", true, 2) ::
      Nil)
      .toDF("id", "key", "cd", "is_active", "hash")
      .write
      .format("io.frama.parisni.spark.postgres")
      .option("url", getPgUrl)
      .option("table", "scd1tablestring")
      .save

    val df = ((1L, "abc_123", "jim") ::
      Nil).toDF("id", "key", "cd")

    df.write
      .format("io.frama.parisni.spark.postgres")
      .option("url", getPgUrl)
      .option("type", "scd1")
      .option("joinKey", "key")
      .option("partitions", 4)
      .option("numSplits", 40)
      .option("table", "scd1tablestring")
      .save

    val result = ((1L, "abc_123", "jim", true) ::
      (3L, "abc_321", "bob", true) ::
      Nil).toDF("id", "key", "cd", "is_active")

    checkAnswer(
      spark.read
        .format("io.frama.parisni.spark.postgres")
        .option("url", getPgUrl)
        .option("query", "select * from scd1tablestring")
        .load
        .select("id", "key", "cd", "is_active"),
      result
    )
  }

}
