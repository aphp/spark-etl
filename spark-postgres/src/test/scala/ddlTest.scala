package io.frama.parisni.spark.postgres

import com.opentable.db.postgres.junit.{EmbeddedPostgresRules, SingleInstancePostgresRule}
import org.apache.spark.sql.QueryTest
import org.junit.{Rule, Test}

import scala.annotation.meta.getter

class ExampleSuite extends QueryTest with SparkSessionTestWrapper {


  @Test def verifySpark(): Unit = {
    spark.sql("select 1").show
  }

  @Test def verifyPostgres() { // Uses JUnit-style assertions
    println(pg.getEmbeddedPostgres.getJdbcUrl("postgres", "pg"))
    val con = pg.getEmbeddedPostgres.getPostgresDatabase.getConnection
    val res2 = con.createStatement().executeUpdate("create table test(i int)")
    val res = con.createStatement().executeQuery("select 27")
    while (res.next())
      println(res.getInt(1))
  }

  @Test def verifySparkPostgres(): Unit = {

    val input = spark.sql("select 1 as t")
    input
      .write.format("postgres")
      .option("host", "localhost")
      .option("port", pg.getEmbeddedPostgres.getPort)
      .option("database", "postgres")
      .option("user", "postgres")
      .option("table", "test_table")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save

    val output = spark.read.format("postgres")
      .option("host", "localhost")
      .option("port", pg.getEmbeddedPostgres.getPort)
      .option("database", "postgres")
      .option("user", "postgres")
      .option("query", "select * from test_table")
      .load

    checkAnswer(input, output)
  }

  @Test
  def verifyPostgresConnectionWithUrl(): Unit = {

    val input = spark.sql("select 2 as t")
    input
      .write.format("postgres")
      .option("url", getPgUrl)
      .option("table", "test_table")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save

  }

  @Test
  def verifyPostgresConnection() {
    val pg = PGTool(spark, getPgUrl, "/tmp")
      .setPassword("postgres")
    pg.showPassword()
    pg.sqlExecWithResult("select 1").show

  }

  @Test
  def verifyPostgresConnectionFailWhenBadPassword() {
    assertThrows[Exception](
      spark.sql("select 2 as t")
        .write.format("postgres")
        .option("host", "localhost")
        .option("port", pg.getEmbeddedPostgres.getPort)
        .option("database", "postgres")
        .option("user", "idontknow")
        .option("password", "badpassword")
        .option("table", "test_table")
        .mode(org.apache.spark.sql.SaveMode.Overwrite)
        .save
    )

  }
}

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  // looks like crazy but compatibility issue with junit rule (public)
  @(Rule@getter)
  var pg: SingleInstancePostgresRule = EmbeddedPostgresRules.singleInstance()

  def getPgUrl = pg.getEmbeddedPostgres.getJdbcUrl("postgres", "postgres") + "&current_schema=public"

  def getPgTool() = PGTool(spark, getPgUrl, "/tmp")

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

}
