package io.frama.parisni.spark.sync.analyze

import java.sql.Timestamp

import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.{DataFrame, QueryTest, SparkSession}

import scala.io.Source
import AnalyzeTableYaml._
import io.frama.parisni.spark.sync.copy.SparkSessionTestWrapper

class AnalyzeTableTest extends QueryTest with SparkSessionTestWrapper {

  //@Test
  def analyzeTest(): Unit = {
    println("Before io.frama.parisni.spark.sync.AnalyzeTableTest -----------------------")
    //Create test tables
    createTables()

    val filename = "analyzeTable.yaml" //args(0)
    val ymlTxt = Source.fromFile(filename).mkString
    val yaml = ymlTxt.stripMargin.parseYaml
    val database = yaml.convertTo[Database]

    // Spark Session
    val spark = SparkSession.builder()
      .master("local")
      .appName("spark session")
      //.config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val deltaPath = database.deltaPath
    val host = database.host.toString
    val port = database.port.toString
    val db = database.db.toString
    val user = database.user.toString
    val schema = database.schema.toString
    val pw = database.pw.getOrElse("")

    val analyze: AnalyzeTable = new AnalyzeTable
    analyze.analyzeTables(spark, deltaPath, host, port, user, schema, db, pw)


    /*val anal = new io.frama.parisni.spark.sync.analyze.AnalyzeTable
      anal.analyzeTables(spark, deltaPath="/tmp", host="localhost", port="5432", user="openpg", pw="openpgpwd",
      schema="public", db="postgres")*/
    /*anal.analyzeTables(spark, host="localhost", port=pg.getEmbeddedPostgres.getPort.toString, user="postgres",
      schema="public", db="postgres")*/

    println("After io.frama.parisni.spark.sync.AnalyzeTableTest -----------------------")
  }


  def createTables(): Unit = {

    //println(pg.getEmbeddedPostgres.getJdbcUrl("postgres", "postgres"))
    //val url = f"jdbc:postgresql://localhost:${pg.getEmbeddedPostgres.getPort}/postgres?user=postgres&currentSchema=public"
    val url = f"jdbc:postgresql://localhost:5432/postgres?user=openpg&password=openpgpwd&currentSchema=public"
    import spark.implicits._
    // Create table "meta_table"
    val metaTableDF: DataFrame = (
      (1, "spark-prod", "t1", Timestamp.valueOf("2020-03-16 00:00:00"), -1) ::
        (2, "spark-prod", "t2", Timestamp.valueOf("2020-03-16 00:00:00"), -1) ::
        (3, "db-test", "t3", Timestamp.valueOf("2020-03-16 00:00:00"), -1) ::
        (4, "spark-prod", "t4", Timestamp.valueOf("2020-03-16 00:00:00"), -1) ::
        Nil).toDF("ids_table", "lib_database", "lib_table", "last_analyze", "hash")
    metaTableDF.write.format("postgres")
      .option("url", url)
      .option("table", "meta_table")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save
    spark.read.format("postgres")
      .option("url", url)
      .option("query", "select * from meta_table")
      .load.show

    // Create table "meta_column"
    val metaColumnDF: DataFrame = (
      (1, 1, "t1", "c1t1", -1) ::
        (2, 1, "t1", "c2t1", -1) ::
        (3, 2, "t2", "c1t2", -1) ::
        (4, 2, "t2", "c2t2", -1) ::
        (5, 3, "t3", "c1t3", -1) ::
        (6, 3, "t3", "c2t3", -1) ::
        (7, 4, "t4", "c1t4", -1) ::
        (8, 4, "t4", "c2t4", -1) ::
        Nil).toDF("ids_column", "ids_table", "lib_table", "lib_column", "hash")

    metaColumnDF.write.format("postgres")
      .option("url", url)
      .option("table", "meta_column")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save

    spark.read.format("postgres")
      .option("url", url)
      .option("query", "select * from meta_column")
      .load.show


    // Create table "t1"
    val t1DF: DataFrame = (
      (1, "text l1", "date l1") :: (2, "text l2", "date l2") :: (3, "text l3", "date l3") :: (4, "text l4", "date l4") ::
        Nil).toDF("id", "c1t1", "c2t1")
    val t1Path = "/tmp/t1"
    t1DF.write.format("delta").mode("overwrite").save(t1Path)
    //spark.sql(s"CREATE TABLE t1 USING DELTA LOCATION '${t1Path}'")
    spark.read.format("delta").load(t1Path).show

    /*t1DF.write.format("postgres")
      .option("url", url)
      .option("table", "t1")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save
    spark.read.format("postgres")
      .option("url", url)
      .option("query", "select * from t1")
      .load.show*/


    // Create table "t2"
    val t2DF: DataFrame = (
      (1, "text l1", "date l1") :: (2, "text l2", "date l2") :: (3, "text l3", "date l3") :: (4, "text l4", "date l4") ::
        Nil).toDF("id", "c1t2", "c2t2")
    val t2Path = "/tmp/t2"
    t2DF.write.format("delta").mode("overwrite").save(t2Path)
    //spark.sql(s"CREATE TABLE t2 USING DELTA LOCATION '${t2Path}'")
    spark.read.format("delta").load(t2Path).show
    /*t2DF.write.format("postgres")
      .option("url", url)
      .option("table", "t2")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save
    spark.read.format("postgres")
      .option("url", url)
      .option("query", "select * from t2")
      .load.show*/


    // Create table "t3"
    val t3DF: DataFrame = (
      (1, "text l1", "date l1") :: (2, "text l2", "date l2") :: (3, "text l3", "date l3") :: (4, "text l4", "date l4") ::
        Nil).toDF("id", "c1t3", "c2t3")
    val t3Path = "/tmp/t3"
    t3DF.write.format("delta").mode("overwrite").save(t3Path)
    //spark.sql(s"CREATE TABLE t3 USING DELTA LOCATION '${t3Path}'")
    spark.read.format("delta").load(t3Path).show
    /*t3DF.write.format("postgres")
      .option("url", url)
      .option("table", "t3")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save
    spark.read.format("postgres")
      .option("url", url)
      .option("query", "select * from t3")
      .load.show*/


    // Create table "t4"
    val t4DF: DataFrame = (
      (1, "text l1", "date l1") :: (2, "text l2", "date l2") :: (3, "text l3", "date l3") :: (4, "text l4", "date l4") ::
        Nil).toDF("id", "c1t4", "c2t4")
    val t4Path = "/tmp/t4"
    t4DF.write.format("delta").mode("overwrite").save(t4Path)
    //spark.sql(s"CREATE TABLE t4 USING DELTA LOCATION '${t4Path}'")
    spark.read.format("delta").load(t4Path).show
    /*t4DF.write.format("postgres")
      .option("url", url)
      .option("table", "t4")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save
    spark.read.format("postgres")
      .option("url", url)
      .option("query", "select * from t4")
      .load.show*/
  }
}
