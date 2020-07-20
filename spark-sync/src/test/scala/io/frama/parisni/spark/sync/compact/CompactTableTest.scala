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
    println(
      "Before io.frama.parisni.spark.sync.compact.CompactTableTest -----------------------"
    )
    //Create test tables
    createTables()
    // Spark Session

    val filename = "compactTable.yaml"
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
    compTab.compactTables(
      spark,
      deltaPath,
      partition,
      numFiles,
      host,
      port,
      user,
      schema,
      db,
      pw
    )

    println(
      "After io.frama.parisni.spark.sync.compact.CompactTableTest -----------------------"
    )
  }

  def createTables(): Unit = {

    val url = getPgUrl
    val comp1Path = "/tmp/comp1"
    val comp2Path = "/tmp/comp2"
    val comp3Path = "/tmp/comp3"
    val comp4Path = "/tmp/comp4"

    import spark.implicits._
    // Create table "meta_table"
    val metaTableDF: DataFrame = ((
      1,
      "spark-prod",
      "t1",
      Timestamp.valueOf("2020-03-16 00:00:00"),
      -1,
      "delta",
      comp1Path
    ) ::
      (
        2,
        "spark-prod",
        "t2",
        Timestamp.valueOf("2020-03-17 00:00:00"),
        -1,
        "postgres",
        comp2Path
      ) ::
      (
        3,
        "db-test",
        "t3",
        Timestamp.valueOf("2020-03-18 00:00:00"),
        -1,
        "delta",
        comp3Path
      ) ::
      (
        4,
        "spark-prod",
        "t4",
        Timestamp.valueOf("2020-03-19 00:00:00"),
        -1,
        "delta",
        comp4Path
      ) ::
      Nil).toDF(
      "ids_table",
      "lib_database",
      "lib_table",
      "last_analyze",
      "hash",
      "type_table",
      "path_table"
    )
    metaTableDF.write
      .format("postgres")
      .option("url", url)
      .option("table", "meta_table")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save
    spark.read
      .format("postgres")
      .option("url", url)
      .option("query", "select * from meta_table")
      .load
      .show

    // Create table "/tmp/comp1"
    val comp1DF: DataFrame = ((1, "text l1", "date l1") :: (
      2,
      "text l2",
      "date l2"
    ) :: (3, "text l3", "date l3") :: (4, "text l4", "date l4") ::
      (5, "text l5", "date l5") :: (6, "text l6", "date l6") :: (
      7,
      "text l7",
      "date l7"
    ) :: (8, "text l8", "date l8") ::
      (9, "text 19", "date l9") :: (10, "text l10", "date l10") :: (
      11,
      "text l11",
      "date l11"
    ) :: (12, "text l12", "date l12") ::
      (13, "text l13", "date l13") :: (14, "text l14", "date l14") :: (
      15,
      "text l15",
      "date l15"
    ) :: (16, "text l16", "date l16") ::
      (17, "text l17", "date l17") :: (18, "text l18", "date l18") :: (
      19,
      "text l19",
      "date l19"
    ) :: (20, "text l20", "date l20") ::
      (21, "text l21", "date l21") :: (22, "text l22", "date l22") :: (
      23,
      "text l23",
      "date l23"
    ) :: (24, "text l24", "date l24") ::
      (25, "text l25", "date l25") :: (26, "text l26", "date l26") :: (
      27,
      "text 127",
      "date l27"
    ) :: (28, "text l28", "date l28") ::
      (29, "text l29", "date l29") :: (30, "text l30", "date l30") :: (
      31,
      "text l31",
      "date l31"
    ) :: (32, "text l32", "date l32") ::
      (33, "text l33", "date l33") :: (34, "text l34", "date l34") :: (
      35,
      "text l35",
      "date l35"
    ) :: (36, "text l36", "date l36") ::
      (37, "text l37", "date l37") :: (38, "text l38", "date l38") :: (
      39,
      "text l39",
      "date l39"
    ) :: (40, "text l40", "date l40") ::
      Nil).toDF("id", "c1t1", "c2t1")
    comp1DF.write.format("delta").mode("overwrite").save(comp1Path)
    spark.read.format("delta").load(comp1Path).show

    // Create table "/tmp/comp2"
    val t2DF: DataFrame = ((1, "text 21", "date 21") :: (
      2,
      "text 22",
      "date 22"
    ) :: (3, "text 23", "date 23") :: (4, "text 24", "date 24") ::
      Nil).toDF("id", "c1t2", "c2t2")
    t2DF.write.format("delta").mode("overwrite").save(comp2Path)
    spark.read.format("delta").load(comp2Path).show

    // Create table "/tmp/comp3"
    val t3DF: DataFrame = ((1, "text 31", "date 31") :: (
      2,
      "text 32",
      "date 32"
    ) :: (3, "text 33", "date 33") :: (4, "text 34", "date 34") ::
      Nil).toDF("id", "c1t3", "c2t3")
    t3DF.write.format("delta").mode("overwrite").save(comp3Path)
    spark.read.format("delta").load(comp3Path).show

    // Create table "/tmp/comp4"
    val t4DF: DataFrame = ((1, "text 41", "date 41") :: (
      2,
      "text 42",
      "date 42"
    ) :: (3, "text 43", "date 43") :: (4, "text 44", "date 44") ::
      Nil).toDF("id", "c1t4", "c2t4")
    t4DF.write.format("delta").mode("overwrite").save(comp4Path)
    spark.read.format("delta").load(comp4Path).show
  }
}
