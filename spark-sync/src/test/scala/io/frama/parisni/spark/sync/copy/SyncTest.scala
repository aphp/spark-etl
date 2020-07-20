package io.frama.parisni.spark.sync.copy

import java.sql.Timestamp
import org.junit.Test
import io.frama.parisni.spark.sync.Sync
import org.apache.spark.sql.DataFrame

class SyncTest extends SolrConfTest {

  //@Test
  def verifySyncDeltaToPostgres(): Unit = {

    println("io.frama.parisni.spark.sync.Sync Delta To Postgres")
    val deltaConfTest: DeltaConfTest = new DeltaConfTest
    deltaConfTest.createDeltaTables

    val url =
      f"jdbc:postgresql://localhost:${pg.getEmbeddedPostgres.getPort}/postgres?user=postgres&currentSchema=public"

    /*val pgConfTest:io.frama.parisni.spark.sync.copy.PostgresConfTest = new io.frama.parisni.spark.sync.copy.PostgresConfTest
    pgConfTest.createPostgresTables*/

    /*// Create table "target"
    val tInputDF = (
      (1, "id1t", 1, "PG details of 1st row target", Timestamp.valueOf("2017-06-16 00:00:00"),
        Timestamp.valueOf("2019-06-16 00:00:00"), Timestamp.valueOf("2019-06-16 00:00:00")) ::
        (2, "id2t", 2, "PG details of 2nd row target", Timestamp.valueOf("2018-07-25 00:00:00"),
          Timestamp.valueOf("2019-06-16 00:00:00"), Timestamp.valueOf("2019-06-16 00:00:00")) ::
        (3, "id3t", 3, "PG details of 3rd row target", Timestamp.valueOf("2017-11-19 00:00:00"),
          Timestamp.valueOf("2011-06-16 00:00:00"), Timestamp.valueOf("2019-06-26 23:10:02")) ::
        (4, "id4t", 4, "PG details of 4th row target", Timestamp.valueOf("2017-07-05 00:00:00"),
          Timestamp.valueOf("2019-06-16 00:00:00"), Timestamp.valueOf("2017-06-16 00:00:00")) ::
        (5, "id5", 5, "PG details of 5th row target", Timestamp.valueOf("2019-09-25 20:16:07"),
          Timestamp.valueOf("2019-09-25 20:16:07"), Timestamp.valueOf("2019-06-16 00:00:00")) ::
        (6, "id6", 6, "PG details of 6th row target", Timestamp.valueOf("2013-01-30 00:00:00"),
          Timestamp.valueOf("2019-06-16 00:00:00"), Timestamp.valueOf("2019-06-16 00:00:00")) ::
        Nil).toDF("id", "pk2", "hash", "details", "date_update", "date_update2", "date_update3")

    tInputDF.write.format("postgres")
      .option("url", url)
      .option("table", "target")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save*/

    val config = Map(
      "S_TABLE_NAME" -> "source",
      "S_TABLE_TYPE" -> "delta",
      "S_DATE_FIELD" -> "date_update",
      "PATH" -> "/tmp",
      "T_TABLE_NAME" -> "target",
      "T_TABLE_TYPE" -> "postgres",
      "HOST" -> "localhost",
      "PORT" -> s"${pg.getEmbeddedPostgres.getPort}",
      "DATABASE" -> "postgres",
      "USER" -> "postgres",
      "SCHEMA" -> "public",
      "T_LOAD_TYPE" -> "full" //, "T_DATE_MAX" -> "2018-10-16 23:16:16"
    )

    val dates = List("date_update", "date_update2", "date_update3")
    val pks = List("id", "pk2")

    val sync = new Sync()
    sync.syncSourceTarget(spark, config, dates, pks)

    println("Postgres after update")
    spark.read
      .format("postgres")
      .option("url", url)
      .option("query", "select * from target")
      .load
      .show

  }

  @Test //*** Error if loadType = full -> table not created by DFTool.saveHive
  def verifySyncPostgresToDelta(): Unit = {

    import spark.implicits._
    println("io.frama.parisni.spark.sync.Sync Postgres To Delta")
    val url =
      f"jdbc:postgresql://localhost:${pg.getEmbeddedPostgres.getPort}/postgres?user=postgres&currentSchema=public"

    // Create table "source"
    val sInputDF: DataFrame = ((
      1,
      "id1s",
      "*PG details of 1st row source",
      Timestamp.valueOf("2016-02-01 23:00:01"),
      Timestamp.valueOf("2016-06-16 00:00:00"),
      Timestamp.valueOf("2016-06-16 00:00:00")
    ) ::
      (
        2,
        "id2s",
        "--PG details of 2nd row source",
        Timestamp.valueOf("2017-06-05 23:00:01"),
        Timestamp.valueOf("2016-06-16 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00")
      ) ::
      (
        3,
        "id3s",
        "*PG details of 3rd row source",
        Timestamp.valueOf("2017-08-07 23:00:01"),
        Timestamp.valueOf("2016-06-16 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00")
      ) ::
      (
        4,
        "id4s",
        "--PG details of 4th row source",
        Timestamp.valueOf("2018-10-16 23:00:01"),
        Timestamp.valueOf("2016-06-16 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00")
      ) ::
      (
        5,
        "id5",
        "*PG details of 5th row source",
        Timestamp.valueOf("2019-12-27 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00")
      ) ::
      (
        6,
        "id6",
        "--PG details of 6th row source",
        Timestamp.valueOf("2020-01-14 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00")
      ) ::
      Nil).toDF(
      "id",
      "pk2",
      "details",
      "date_update",
      "date_update2",
      "date_update3"
    )

    sInputDF.write
      .format("postgres")
      .option("url", url)
      .option("table", "source")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save

    val config = Map(
      "S_TABLE_NAME" -> "source",
      "S_TABLE_TYPE" -> "postgres",
      "S_DATE_FIELD" -> "date_update",
      "HOST" -> "localhost",
      "PORT" -> s"${pg.getEmbeddedPostgres.getPort}",
      "DATABASE" -> "postgres",
      "USER" -> "postgres",
      "SCHEMA" -> "public",
      "T_TABLE_NAME" -> "target",
      "T_TABLE_TYPE" -> "delta",
      "PATH" -> "/tmp",
      "T_LOAD_TYPE" -> "full" //, "T_DATE_MAX" -> "2018-10-16 23:16:16"
    )

    val dates = List("date_update", "date_update2", "date_update3")
    val pks = List("id", "pk2")

    val sync = new Sync()
    sync.syncSourceTarget(spark, config, dates, pks)

    /*println("Delta after update")
    spark.read.format("delta").load("/tmp/target").show*/

  }

  //@Test
  def verifySyncDeltaToSolr(): Unit = {

    println("io.frama.parisni.spark.sync.Sync Delta to Solr")
    val deltaConfTest: DeltaConfTest = new DeltaConfTest
    deltaConfTest.createDeltaTables

    //StartSolrCloudCluster & CreateSolrTables
    createSolrTables

    val config = Map(
      "S_TABLE_NAME" -> "source",
      "S_TABLE_TYPE" -> "delta",
      "S_DATE_FIELD" -> "date_update",
      "PATH" -> "/tmp",
      "T_TABLE_NAME" -> "target",
      "T_TABLE_TYPE" -> "solr",
      "ZKHOST" -> zkHost,
      "T_LOAD_TYPE" -> "full",
      "T_DATE_MAX" -> "2018-10-16 23:16:16"
    )

    val dates = List("date_update", "date_update2", "date_update3")
    val pks = List("id", "pk2")

    val sync = new Sync()
    sync.syncSourceTarget(spark, config, dates, pks)

    /*println("Solr after update")
    val res = spark.read.format("solr")
      .options(Map( "collection" -> "target", "zkhost" -> zkHost))
      .load
    res.show*/
  }

  //@Test
  def verifySyncPostgresToSolr(): Unit = {

    println("io.frama.parisni.spark.sync.Sync Postgres To Solr")

    //StartSolrCloudCluster & CreateSolrTables
    createSolrTables

    import spark.implicits._
    val url =
      f"jdbc:postgresql://localhost:${pg.getEmbeddedPostgres.getPort}/postgres?user=postgres&currentSchema=public"

    // Create table "source"
    val sInputDF: DataFrame = ((
      1,
      "id1s",
      "PG details of 1st row source",
      Timestamp.valueOf("2016-02-01 23:00:01"),
      Timestamp.valueOf("2016-06-16 00:00:00"),
      Timestamp.valueOf("2016-06-16 00:00:00")
    ) ::
      (
        2,
        "id2s",
        "PG details of 2nd row source",
        Timestamp.valueOf("2017-06-05 23:00:01"),
        Timestamp.valueOf("2016-06-16 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00")
      ) ::
      (
        3,
        "id3s",
        "PG details of 3rd row source",
        Timestamp.valueOf("2017-08-07 23:00:01"),
        Timestamp.valueOf("2016-06-16 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00")
      ) ::
      (
        4,
        "id4s",
        "PG details of 4th row source",
        Timestamp.valueOf("2018-10-16 23:00:01"),
        Timestamp.valueOf("2016-06-16 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00")
      ) ::
      (
        5,
        "id5",
        "PG details of 5th row source",
        Timestamp.valueOf("2019-12-27 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00")
      ) ::
      (
        6,
        "id6",
        "PG details of 6th row source",
        Timestamp.valueOf("2020-01-14 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00")
      ) ::
      Nil).toDF(
      "id",
      "pk2",
      "details",
      "date_update",
      "date_update2",
      "date_update3"
    )

    sInputDF.write
      .format("postgres")
      .option("url", url)
      .option("table", "source")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save

    val config = Map(
      "S_TABLE_NAME" -> "source",
      "S_TABLE_TYPE" -> "postgres",
      "S_DATE_FIELD" -> "date_update",
      "HOST" -> "localhost",
      "PORT" -> s"${pg.getEmbeddedPostgres.getPort}",
      "DATABASE" -> "postgres",
      "USER" -> "postgres",
      "SCHEMA" -> "public",
      "T_TABLE_NAME" -> "target",
      "T_TABLE_TYPE" -> "solr",
      "ZKHOST" -> zkHost,
      "T_LOAD_TYPE" -> "full" //, "T_DATE_MAX" -> "2018-10-16 23:16:16"
    )

    val dates = List("date_update", "date_update2", "date_update3")
    val pks = List("id", "pk2")

    val sync = new Sync()
    sync.syncSourceTarget(spark, config, dates, pks)

    /*println("Solr after update")
    val res = spark.read.format("solr")
      .options(Map( "collection" -> "target", "zkhost" -> zkHost))
      .load
    res.show*/
  }

  //@Test
  def verifySyncSolrToDelta(): Unit = {

    println("io.frama.parisni.spark.sync.Sync Solr To Delta")
    //StartSolrCloudCluster & CreateSolrTables
    createSolrTables

    val deltaConfTest: DeltaConfTest = new DeltaConfTest
    deltaConfTest.createDeltaTables

    val config = Map(
      "S_TABLE_NAME" -> "source",
      "S_TABLE_TYPE" -> "solr",
      "S_DATE_FIELD" -> "date_update",
      "ZKHOST" -> zkHost,
      "T_TABLE_NAME" -> "target",
      "T_TABLE_TYPE" -> "delta",
      "PATH" -> "/tmp",
      "T_LOAD_TYPE" -> "scd1" //, "T_DATE_MAX" -> "2018-10-16 23:16:16"
    )

    val dates = List("date_update", "date_update2", "date_update3")
    val pks = List("id", "pk2")

    val sync = new Sync()
    sync.syncSourceTarget(spark, config, dates, pks)

  }

  //@Test
  def verifySyncSolrToPostgres(): Unit = {

    println("io.frama.parisni.spark.sync.Sync Solr To Postgres")
    //StartSolrCloudCluster & CreateSolrTables
    createSolrTables

    /*val postgresConfTest:io.frama.parisni.spark.sync.copy.PostgresConfTest = new io.frama.parisni.spark.sync.copy.PostgresConfTest
    postgresConfTest.createPostgresTables*/

    import spark.implicits._
    val url =
      f"jdbc:postgresql://localhost:${pg.getEmbeddedPostgres.getPort}/postgres?user=postgres&currentSchema=public"
    // Create table "target"
    /*val tInputDF = (
      (1, "id1t", 1, "PG details of 1st row target", Timestamp.valueOf("2017-06-16 00:00:00"),
        Timestamp.valueOf("2019-06-16 00:00:00"), Timestamp.valueOf("2019-06-16 00:00:00")) ::
        (2, "id2t", 2, "PG details of 2nd row target", Timestamp.valueOf("2018-07-25 00:00:00"),
          Timestamp.valueOf("2019-06-16 00:00:00"), Timestamp.valueOf("2019-06-16 00:00:00")) ::
        (3, "id3t", 3, "PG details of 3rd row target", Timestamp.valueOf("2017-11-19 00:00:00"),
          Timestamp.valueOf("2011-06-16 00:00:00"), Timestamp.valueOf("2019-06-26 23:10:02")) ::
        (4, "id4t", 4, "PG details of 4th row target", Timestamp.valueOf("2017-07-05 00:00:00"),
          Timestamp.valueOf("2019-06-16 00:00:00"), Timestamp.valueOf("2017-06-16 00:00:00")) ::
        (5, "id5", 5, "PG details of 5th row target", Timestamp.valueOf("2019-09-25 20:16:07"),
          Timestamp.valueOf("2019-09-25 20:16:07"), Timestamp.valueOf("2019-06-16 00:00:00")) ::
        (6, "id6", 6, "PG details of 6th row target", Timestamp.valueOf("2013-01-30 00:00:00"),
          Timestamp.valueOf("2019-06-16 00:00:00"), Timestamp.valueOf("2019-06-16 00:00:00")) ::
        Nil).toDF("id", "pk2", "hash", "details", "date_update", "date_update2", "date_update3")

    tInputDF.write.format("postgres")
      .option("url", url)
      .option("table", "target")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save*/

    val config = Map(
      "S_TABLE_NAME" -> "source",
      "S_TABLE_TYPE" -> "solr",
      "S_DATE_FIELD" -> "date_update",
      "ZKHOST" -> zkHost,
      "T_TABLE_NAME" -> "target",
      "T_TABLE_TYPE" -> "postgres",
      "HOST" -> "localhost",
      "PORT" -> s"${pg.getEmbeddedPostgres.getPort}",
      "DATABASE" -> "postgres",
      "USER" -> "postgres",
      "SCHEMA" -> "public",
      "T_LOAD_TYPE" -> "full" //, "T_DATE_MAX" -> "2018-10-16 23:16:16"
    )

    val dates = List("date_update", "date_update2", "date_update3")
    val pks = List("id", "pk2")

    val sync = new Sync()
    sync.syncSourceTarget(spark, config, dates, pks)

    // Table "target" after update
    spark.read
      .format("postgres")
      .option("url", url)
      .option("query", "select * from target")
      .load
      .show

  }

  //@Test
  def verifySyncPgToPg(): Unit = {

    println("io.frama.parisni.spark.sync.Sync Postgres to Postgres")
    // create Embedded Postgres Tables
    /*val postgresConfTest:io.frama.parisni.spark.sync.copy.PostgresConfTest = new io.frama.parisni.spark.sync.copy.PostgresConfTest
    postgresConfTest.createPostgresTables*/

    val url =
      f"jdbc:postgresql://localhost:${pg.getEmbeddedPostgres.getPort}/postgres?user=postgres&currentSchema=public"
    val config = Map(
      "S_TABLE_NAME" -> "source",
      "S_TABLE_TYPE" -> "postgres",
      "S_DATE_FIELD" -> "date_update",
      "T_TABLE_NAME" -> "target",
      "T_TABLE_TYPE" -> "postgres", //"T_DATE_MAX" -> "2018-10-16 23:16:16",
      "HOST" -> "localhost",
      "PORT" -> s"${pg.getEmbeddedPostgres.getPort}",
      "DATABASE" -> "postgres",
      "USER" -> "postgres",
      "SCHEMA" -> "public",
      "T_LOAD_TYPE" -> "full"
    )
    val dates = List("date_update", "date_update2", "date_update3")
    val pks = List("id", "pk2")

    val sync = new Sync()
    sync.syncSourceTarget(spark, config, dates, pks)

    // Table "target" after update
    /*spark.read.format("postgres")
      .option("url", url)
      .option("query", "select * from target")
      .load.show*/
  }

  //@Test
  def verifySyncDeltaToDelta(): Unit = {

    println("io.frama.parisni.spark.sync.Sync Delta to Delta")
    // Create Delta Table
    val deltaConfTest: DeltaConfTest = new DeltaConfTest
    deltaConfTest.createDeltaTables

    val config = Map(
      "S_TABLE_NAME" -> "source",
      "S_TABLE_TYPE" -> "delta",
      "S_DATE_FIELD" -> "date_update",
      "T_TABLE_NAME" -> "target",
      "T_TABLE_TYPE" -> "delta", //"T_DATE_MAX" -> "2019-12-26 23:16:16",
      "PATH" -> "tmp",
      "T_LOAD_TYPE" -> "scd1"
    )
    val dates = List("date_update", "date_update2", "date_update3")
    val pks = List("id", "pk2")

    val sync = new Sync()
    sync.syncSourceTarget(spark, config, dates, pks)
  }

  //@Test
  def verifSyncSolrToSolr(): Unit = {

    println("io.frama.parisni.spark.sync.Sync Solr to Solr")
    //StartSolrCloudCluster & CreateSolrTables
    createSolrTables

    val config = Map(
      "S_TABLE_NAME" -> "source",
      "S_TABLE_TYPE" -> "solr",
      "S_DATE_FIELD" -> "date_update",
      "T_TABLE_NAME" -> "target",
      "T_TABLE_TYPE" -> "solr", //"T_DATE_MAX" -> "2017-08-07 23:00:00",
      "ZKHOST" -> zkHost,
      "T_LOAD_TYPE" -> "full"
    )
    val dates = List("date_update", "date_update2", "date_update3")
    val pks = List("id", "pk2")

    val sync = new Sync()
    sync.syncSourceTarget(spark, config, dates, pks)
  }
}
