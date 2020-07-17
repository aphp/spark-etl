package io.frama.parisni.spark.sync.copy

import java.sql.Timestamp

import io.frama.parisni.spark.sync.conf.DeltaConf
import org.apache.spark.sql.{DataFrame, QueryTest, SparkSession}
import org.junit.Test

class DeltaConfTest extends QueryTest with SparkSessionTestWrapper {

  //@Test
  def createDeltaTables(): Unit = { // Uses JUnit-style assertions

    import spark.implicits._

    println("Before: Table /tmp/source exists = "+ checkTableExists(spark, "/tmp", "source"))
    // Create table "source"
    val sInputDF: DataFrame = (
      (1, "id1s", 1, "Delta details of 1st row source", Timestamp.valueOf("2016-02-01 23:00:01"),
        Timestamp.valueOf("2016-01-16 00:00:00"), Timestamp.valueOf("2016-02-16 00:00:00")) ::
      (2, "id2s", 2, "Delta details of 2nd row source", Timestamp.valueOf("2017-06-05 23:00:01"),
        Timestamp.valueOf("2016-03-16 00:00:00"), Timestamp.valueOf("2016-04-16 00:00:00")) ::
      (3, "id3s", 3, "Delta details of 3rd row source", Timestamp.valueOf("2017-08-07 23:00:01"),
        Timestamp.valueOf("2016-05-16 00:00:00"), Timestamp.valueOf("2016-06-16 00:00:00")) ::
      (4, "id4s", 4, "Delta details of 4th row source", Timestamp.valueOf("2018-10-16 23:00:01"),
        Timestamp.valueOf("2016-07-16 00:00:00"), Timestamp.valueOf("2016-08-16 00:00:00")) ::
      (5, "id5", 5, "Delta details of 5th row source", Timestamp.valueOf("2019-12-27 00:00:00"),
        Timestamp.valueOf("2016-09-16 00:00:00"), Timestamp.valueOf("2016-10-16 00:00:00")) ::
      (6, "id6", 6, "Delta details of 6th row source", Timestamp.valueOf("2020-01-14 00:00:00"),
        Timestamp.valueOf("2016-11-16 00:00:00"), Timestamp.valueOf("2016-12-16 00:00:00")) ::
      Nil).toDF("id", "pk2", "hash", "details", "date_update", "date_update2", "date_update3")

    val sourceDeltaTable = "/tmp/source"
    sInputDF.write.format("delta")
      .mode("overwrite")
      .save(sourceDeltaTable)

    println("After: table /tmp/source exists = "+ checkTableExists(spark, "/tmp", "source"))
    println("table /tmp/target exists = "+ checkTableExists(spark, "/tmp", "target"))

    val sOutputDF = spark.read.format("delta").load(sourceDeltaTable)
    sOutputDF.show()
    //checkAnswer(sInputDF, sOutputDF)

  }

  //@Test
  def verifySyncDeltaToDelta(): Unit = {

    // Create deltaTable
    createDeltaTables()

    val mapy = Map("S_TABLE_NAME" -> "source", "S_TABLE_TYPE" -> "delta", "S_DATE_FIELD" -> "date_update",
      "T_TABLE_NAME" -> "target", "T_TABLE_TYPE" -> "delta",  "T_DATE_MAX" -> "2019-12-26 23:16:16",
      "PATH" -> "/tmp", "T_LOAD_TYPE" -> "scd1"
    )
    val dates = List("date_update", "date_update2", "date_update3")
    val pks = List("id","pk2")
    val dc = new DeltaConf(mapy, dates, pks)

    val path = dc.getPath.getOrElse("/tmp")
    val sTable = dc.getSourceTableName.getOrElse("")
    val sDateField = dc.getSourceDateField.getOrElse("")
    val tTable = dc.getTargetTableName.getOrElse("")
    val dateMax = dc.getDateMax(spark)
    val loadType = dc.getLoadType.getOrElse("full")

    // load table from source
    println(s"Load data from Table ${sTable}")
    val sDf = dc.readSource(spark, path, sTable, sDateField, dateMax, loadType)
    sDf.show()

    // write table to target
    dc.writeSource(spark, sDf, path, tTable, loadType)

    println(f"Table ${tTable} after update")
    val deltaPath = "%s/%s".format(path, tTable)
    spark.read.format("delta").load(deltaPath).show()
  }

  //@Test
  def verifCalculDateMax(): Unit = {

    createDeltaTables()

    val mapy = Map("S_TABLE_NAME" -> "source", "S_TABLE_TYPE" -> "delta", "S_DATE_FIELD" -> "date_update",
      "T_TABLE_NAME" -> "target", "T_TABLE_TYPE" -> "delta",  //"T_DATE_MAX" -> "2016-10-16 23:16:16",
      "PATH" -> "/tmp", "T_LOAD_TYPE" -> "full"
    )
    val dates = List("date_update", "date_update2", "date_update3")
    val pks = List("id","pk2")
    val dc = new DeltaConf(mapy, dates, pks)

    /*val path = dc.getPath.getOrElse("/tmp")
    val sTable = dc.getSourceTableName.getOrElse("")
    val sDateField = dc.getSourceDateField.getOrElse("")
    val tTable = dc.getTargetTableName.getOrElse("")*/
    val dateMax =  dc.getDateMax(spark)

    assert(dateMax == "2019-09-25 20:16:07.0")        //"2016-10-16 23:16:16"))    "2019-06-26 23:10:02.0"
  }

  def checkTableExists(spark: SparkSession, path: String, table: String): Boolean = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val deltaPath = path + "/" + table
    val res = fs.exists(new org.apache.hadoop.fs.Path(deltaPath))
    println(s"Delta Table ${deltaPath} exists = "+res)
    return res
  }

}
