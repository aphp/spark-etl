package io.frama.parisni.spark.postgres

import org.apache.spark.sql.{DataFrame, QueryTest}
import org.junit.Test

class CrudTest extends QueryTest with SparkSessionTestWrapper {

  @Test
  def verifyParametrised(): Unit = {
    import spark.implicits._

    val df: DataFrame = (("bob", 2, true) ::
      Nil).toDF("colstring", "colint", "colboolean")
    getPgTool().tableCreate("test_crud", df.schema)
    val query =
      "insert into test_crud (colstring, colint, colboolean) values (?, ? ,?)"
    getPgTool().sqlExec(query, List("bob", 1, false))
    val output = spark.read
      .format("io.frama.parisni.spark.postgres")
      .option("host", "localhost")
      .option("port", pg.getEmbeddedPostgres.getPort)
      .option("database", "postgres")
      .option("user", "postgres")
      .option("query", "select * from test_crud")
      .load
    val wanted =
      (("bob", 1, false) :: Nil).toDF("colstring", "colint", "colboolean")
    checkAnswer(output, wanted)

  }

  @Test
  def verifyParametrisedWithResult(): Unit = {
    import spark.implicits._

    val df: DataFrame = (("bob", 2, true) ::
      Nil).toDF("colstring", "colint", "colboolean")
    getPgTool().tableCreate("test_crud2", df.schema)
    val query =
      "insert into test_crud2 (colstring, colint, colboolean) values (?, ? ,?) returning colstring, colint, colboolean"
    val result = getPgTool().sqlExecWithResult(query, List("bob", 1, false))
    val wanted =
      (("bob", 1, false) :: Nil).toDF("colstring", "colint", "colboolean")
    checkAnswer(result, wanted)
  }

  @Test
  def verifyGetColumnType(): Unit = {
    import spark.implicits._

    val df: DataFrame = (("bob", 2, 3L, true) ::
      Nil).toDF("colstring", "colint", "colong", "colboolean")
    getPgTool().tableCreate("get_table_test", df.schema)
    val query =
      """
        |select *
        |from get_table_test
        |""".stripMargin
    val resultString =
      PGTool.getSqlColumnType(spark, getPgUrl, query, "colstring", "postgres")
    assert(resultString == "String")

    val resultLong =
      PGTool.getSqlColumnType(spark, getPgUrl, query, "colong", "postgres")
    assert(resultLong == "Long")

    val resultInt =
      PGTool.getSqlColumnType(spark, getPgUrl, query, "colint", "postgres")
    assert(resultInt == "Integer")

    assertThrows[UnsupportedOperationException] {
      val resultBool = PGTool.getSqlColumnType(spark,
                                               getPgUrl,
                                               query,
                                               "colboolean",
                                               "postgres")
    }
  }
}
