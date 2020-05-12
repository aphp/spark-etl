package io.frama.parisni.spark.sync.copy

import java.sql.Timestamp

import io.frama.parisni.spark.sync.conf.DeltaConf
import org.apache.spark.sql.{DataFrame, QueryTest, SparkSession}

class DeltaConfTest extends QueryTest with SparkSessionTestWrapper {

  //@Test
  def createDeltaTables(): Unit = { // Uses JUnit-style assertions

    import spark.implicits._

    println("Before: Table /tmp/source exists = "+ checkTableExists(spark, "/tmp", "source"))
    //val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    // Create table "source"
    val s_inputDF: DataFrame = (
      (1, "id1s", 1, "Delta details of 1st row source", Timestamp.valueOf("2016-02-01 23:00:01"),
        Timestamp.valueOf("2016-06-16 00:00:00"), Timestamp.valueOf("2016-06-16 00:00:00")) ::
      (2, "id2s", 2, "Delta details of 2nd row source", Timestamp.valueOf("2017-06-05 23:00:01"),
        Timestamp.valueOf("2016-06-16 00:00:00"), Timestamp.valueOf("2016-06-16 00:00:00")) ::
      (3, "id3s", 3, "Delta details of 3rd row source", Timestamp.valueOf("2017-08-07 23:00:01"),
        Timestamp.valueOf("2016-06-16 00:00:00"), Timestamp.valueOf("2016-06-16 00:00:00")) ::
      (4, "id4s", 4, "Delta details of 4th row source", Timestamp.valueOf("2018-10-16 23:00:01"),
        Timestamp.valueOf("2016-06-16 00:00:00"), Timestamp.valueOf("2016-06-16 00:00:00")) ::
      (5, "id5", 5, "Delta details of 5th row source", Timestamp.valueOf("2019-12-27 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00"), Timestamp.valueOf("2016-06-16 00:00:00")) ::
      (6, "id6", 6, "Delta details of 6th row source", Timestamp.valueOf("2020-01-14 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00"), Timestamp.valueOf("2016-06-16 00:00:00")) ::
      Nil).toDF("id", "pk2", "hash", "details", "date_update", "date_update2", "date_update3")

    val sourceDeltaTable = "/tmp/source"
    s_inputDF.write.format("delta")
      .mode("overwrite")
      .save(sourceDeltaTable)

    println("After: table /tmp/source exists = "+ checkTableExists(spark, "/tmp", "source"))
    println(s"table /tmp/target exists = "+ checkTableExists(spark, "/tmp", "target"))

    val s_outputDF = spark.read.format("delta").load(sourceDeltaTable)
    s_outputDF.show()
    //checkAnswer(s_inputDF, s_outputDF)

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
    val s_table = dc.getSourceTableName.getOrElse("")
    val s_date_field = dc.getSourceDateField.getOrElse("")
    val t_table = dc.getTargetTableName.getOrElse("")
    val date_max = dc.getDateMax(spark)        //pgc.getDateMax.getOrElse("2019-01-01")
    val load_type = dc.getLoadType.getOrElse("full")

    // load table from source
    println(s"Load data from Table ${s_table}")
    val s_df = dc.readSource(spark, path, s_table, s_date_field, date_max, load_type)
    s_df.show()

    // write table to target
    dc.writeSource(spark, s_df, path, t_table, load_type)

    println(f"Table ${t_table} after update")
    val deltaPath = "%s/%s".format(path, t_table)
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

    val path = dc.getPath.getOrElse("/tmp")
    val s_table = dc.getSourceTableName.getOrElse("")
    val s_date_field = dc.getSourceDateField.getOrElse("")
    val t_table = dc.getTargetTableName.getOrElse("")
    val date_max =  dc.getDateMax(spark)        //pgc.getDateMax.getOrElse("2019-01-01")

    assert(date_max == "2019-09-25 20:16:07.0")        //"2016-10-16 23:16:16"))    "2019-06-26 23:10:02.0"
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
