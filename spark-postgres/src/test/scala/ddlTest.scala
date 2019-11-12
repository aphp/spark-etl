package io.frama.parisni.spark.postgres

import com.opentable.db.postgres.junit.{EmbeddedPostgresRules, SingleInstancePostgresRule}
import org.junit.{Rule, Test}
import org.scalatest.junit.AssertionsForJUnit

import scala.annotation.meta.getter

class ExampleSuite extends AssertionsForJUnit with SparkSessionTestWrapper {

  // looks like crazy but compatibility issue with junit rule (public)
  @(Rule@getter)
  var pg: SingleInstancePostgresRule = EmbeddedPostgresRules.singleInstance();


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

    val schema = pg.getEmbeddedPostgres.getPostgresDatabase.getConnection.getSchema
    println(s"schema: $schema")
    spark.sql("select 1 as t")
      .write.format("postgres")
      .option("host", "localhost")
      .option("port", pg.getEmbeddedPostgres.getPort)
      .option("database", "postgres")
      .option("user", "postgres")
      .option("table", "test_table")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save

      spark.read.format("postgres")
      .option("host", "localhost")
      .option("port", pg.getEmbeddedPostgres.getPort)
      .option("database", "postgres")
      .option("user", "postgres")
      .option("query", "select * from test_table")
      .load.show
  }

}

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

}
