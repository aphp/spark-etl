package io.frama.parisni.spark.sync.compact

import java.sql.Timestamp

import io.frama.parisni.spark.sync.compact.CompactTableYaml._
import io.frama.parisni.spark.sync.copy.SparkSessionTestWrapper
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.junit.Test

import scala.io.Source

class CompactTableTest extends QueryTest with SparkSessionTestWrapper {

  @Test
  def compactTest(): Unit = {
    println("Before io.frama.parisni.spark.sync.compact.CompactTableTest -----------------------")
    //Create test tables
    createTables()
    // Spark Session

    val filename = "compactTable.yaml" //args(0)
    val ymlTxt = Source.fromFile(filename).mkString
    val yaml = ymlTxt.stripMargin.parseYaml
    val database = yaml.convertTo[Database]

    val deltaPath = database.deltaPath
    val numFiles = database.numFiles
    val partition = database.partition.getOrElse("")
    val host = database.host.toString
    val port = pg.getEmbeddedPostgres.getPort.toString
    val db = database.db.toString
    val user = database.user.toString
    val schema = database.schema.toString
    val pw = database.pw.getOrElse("")

    val compTab: CompactTable = new CompactTable
    compTab.compactTables(spark, deltaPath, partition, numFiles, host, port, user, schema, db, pw)


    /*val comp = new io.frama.parisni.spark.sync.compact.CompactTable
    comp.compactTables(spark, deltaPath="/tmp", host="localhost", port="5432", user="openpg", pw="openpgpwd",
      schema="public", db="postgres", numFiles=4)*/
    /*anal.analyzeTables(spark, host="localhost", port=pg.getEmbeddedPostgres.getPort.toString, user="postgres",
      schema="public", db="postgres", , numFiles=4)*/

    println("After io.frama.parisni.spark.sync.compact.CompactTableTest -----------------------")
  }


  def createTables(): Unit = {

    val url = getPgUrl
    val comp1Path = "/tmp/comp1"
    val comp2Path = "/tmp/comp2"
    val comp3Path = "/tmp/comp3"
    val comp4Path = "/tmp/comp4"

    import spark.implicits._
    // Create table "meta_table"
    val metaTableDF: DataFrame = (
      (1, "spark-prod", "t1", Timestamp.valueOf("2020-03-16 00:00:00"), -1, "delta", comp1Path) ::
        (2, "spark-prod", "t2", Timestamp.valueOf("2020-03-16 00:00:00"), -1, "postgres", comp2Path) ::
        (3, "db-test", "t3", Timestamp.valueOf("2020-03-16 00:00:00"), -1, "delta", comp3Path) ::
        (4, "spark-prod", "t4", Timestamp.valueOf("2020-03-16 00:00:00"), -1, "delta", comp4Path) ::
        Nil).toDF("ids_table", "lib_database", "lib_table", "last_analyze", "hash", "type_table", "path_table")
    metaTableDF.write.format("postgres")
      .option("url", url)
      .option("table", "meta_table")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save
    spark.read.format("postgres")
      .option("url", url)
      .option("query", "select * from meta_table")
      .load.show

    // Create table "/tmp/comp1"
    val comp1DF: DataFrame = (
      (1, "text l1", "date l1") :: (2, "text l2", "date l2") :: (3, "text l3", "date l3") :: (4, "text l4", "date l4") ::
        (5, "text l1", "date l1") :: (6, "text l2", "date l2") :: (7, "text l3", "date l3") :: (8, "text l4", "date l4") ::
        (9, "text l1", "date l1") :: (10, "text l2", "date l2") :: (11, "text l3", "date l3") :: (12, "text l4", "date l4") ::
        (13, "text l1", "date l1") :: (14, "text l2", "date l2") :: (15, "text l3", "date l3") :: (16, "text l4", "date l4") ::
        (17, "text l1", "date l1") :: (18, "text l2", "date l2") :: (19, "text l3", "date l3") :: (20, "text l4", "date l4") ::
        (21, "text l1", "date l1") :: (22, "text l2", "date l2") :: (23, "text l3", "date l3") :: (24, "text l4", "date l4") ::
        (25, "text l1", "date l1") :: (26, "text l2", "date l2") :: (27, "text l3", "date l3") :: (28, "text l4", "date l4") ::
        (29, "text l1", "date l1") :: (30, "text l2", "date l2") :: (31, "text l3", "date l3") :: (32, "text l4", "date l4") ::
        (33, "text l1", "date l1") :: (34, "text l2", "date l2") :: (35, "text l3", "date l3") :: (36, "text l4", "date l4") ::
        (37, "text l1", "date l1") :: (38, "text l2", "date l2") :: (39, "text l3", "date l3") :: (40, "text l4", "date l4") ::
        Nil).toDF("id", "c1t1", "c2t1")
    comp1DF.write.format("delta").mode("overwrite").save(comp1Path)
    spark.read.format("delta").load(comp1Path).show

    // Create table "/tmp/comp2"
    val t2DF: DataFrame = (
      (1, "text l1", "date l1") :: (2, "text l2", "date l2") :: (3, "text l3", "date l3") :: (4, "text l4", "date l4") ::
        Nil).toDF("id", "c1t2", "c2t2")
    t2DF.write.format("delta").mode("overwrite").save(comp2Path)
    spark.read.format("delta").load(comp2Path).show

    // Create table "/tmp/comp3"
    val t3DF: DataFrame = (
      (1, "text l1", "date l1") :: (2, "text l2", "date l2") :: (3, "text l3", "date l3") :: (4, "text l4", "date l4") ::
        Nil).toDF("id", "c1t3", "c2t3")
    t3DF.write.format("delta").mode("overwrite").save(comp3Path)
    spark.read.format("delta").load(comp3Path).show

    // Create table "/tmp/comp4"
    val t4DF: DataFrame = (
      (1, "text l1", "date l1") :: (2, "text l2", "date l2") :: (3, "text l3", "date l3") :: (4, "text l4", "date l4") ::
        Nil).toDF("id", "c1t4", "c2t4")
    t4DF.write.format("delta").mode("overwrite").save(comp4Path)
    spark.read.format("delta").load(comp4Path).show
  }
}
