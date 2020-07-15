package io.frama.parisni.spark.sync.copy

import java.sql.Timestamp

import com.opentable.db.postgres.junit.{EmbeddedPostgresRules, SingleInstancePostgresRule}
import io.frama.parisni.spark.postgres.PGTool
import io.frama.parisni.spark.sync.Sync
import io.frama.parisni.spark.sync.conf.PostgresConf
import io.frama.parisni.spark.sync.copy.PostgresToDeltaYaml.Database
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.junit.Rule

import scala.annotation.meta.getter
import scala.io.Source


class PostgresConfTest extends QueryTest with SparkSessionTestWrapper {

  // create Embedded Postgres Tables
  //@Test
  def createPostgresTables(): Unit = { // Uses JUnit-style assertions

    import spark.implicits._

    //val con = pg.getEmbeddedPostgres.getPostgresDatabase.getConnection
    //val url = f"jdbc:postgresql://localhost:${pg.getEmbeddedPostgres.getPort}/postgres?user=postgres&currentSchema=public"
    val url = getPgUrl
    // Create table "source"
    val s_inputDF: DataFrame = (
      (1, "id1s", "test details of 1st row source", Timestamp.valueOf("2016-02-01 23:00:01"),
        Timestamp.valueOf("2016-06-16 00:00:00"), Timestamp.valueOf("2016-06-16 00:00:00")) ::
        (2, "id2s", "test details of 2nd row source", Timestamp.valueOf("2017-06-05 23:00:01"),
          Timestamp.valueOf("2016-06-16 00:00:00"), Timestamp.valueOf("2016-06-16 00:00:00")) ::
        (3, "id3s", "test details of 3rd row source", Timestamp.valueOf("2017-08-07 23:00:01"),
          Timestamp.valueOf("2016-06-16 00:00:00"), Timestamp.valueOf("2016-06-16 00:00:00")) ::
        (4, "id4s", "test details of 4th row source", Timestamp.valueOf("2018-10-16 23:00:01"),
          Timestamp.valueOf("2016-06-16 00:00:00"), Timestamp.valueOf("2016-06-16 00:00:00")) ::
        (5, "id5", "test details of 5th row source", Timestamp.valueOf("2019-12-27 00:00:00"),
          Timestamp.valueOf("2016-06-16 00:00:00"), Timestamp.valueOf("2016-06-16 00:00:00")) ::
        (6, "id6", "test details of 6th row source", Timestamp.valueOf("2020-01-14 00:00:00"),
          Timestamp.valueOf("2016-06-16 00:00:00"), Timestamp.valueOf("2016-06-16 00:00:00")) ::
        Nil).toDF("id", "pk2", "details", "date_update", "date_update2", "date_update3")

    s_inputDF.write.format("jdbc") //postgres
      .option("url", url)
      .option("dbtable", "source") //table
      //.option("driver", "org.postgresql.Driver")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save

    val s_outputDF: DataFrame = spark.read.format("jdbc") //postgres
      .option("url", url)
      .option("query", "select * from source")
      .load

    s_outputDF.show

    println("Table source exists = " + checkTableExists(spark, url, "source", "public"))
    println("Table target exists = " + checkTableExists(spark, url, "target", "public"))
  }

  //@Test
  def verifySyncPgToPg(): Unit = {

    // create Embedded Postgres Tables
    createPostgresTables()

    val mapy = Map("S_TABLE_NAME" -> "source", "S_TABLE_TYPE" -> "postgres", "S_DATE_FIELD" -> "date_update",
      "T_TABLE_NAME" -> "target", "T_TABLE_TYPE" -> "postgres", //"T_DATE_MAX" -> "2018-10-16 23:16:16",
      "HOST" -> "localhost", "PORT" -> s"${pg.getEmbeddedPostgres.getPort}", "DATABASE" -> "postgres", "USER" -> "postgres",
      "SCHEMA" -> "public", "T_LOAD_TYPE" -> "full"
    )
    val dates = List("date_update", "date_update2", "date_update3")
    val pks = List("id", "pk2")
    val pgc = new PostgresConf(mapy, dates, pks)

    val host = pgc.getHost.getOrElse("localhost")
    val port = pg.getEmbeddedPostgres.getPort.toString
    val db = pgc.getDB.getOrElse("postgres")
    val user = pgc.getUser.getOrElse("postgres")
    val schema = pgc.getSchema.getOrElse("public")
    val s_table = pgc.getSourceTableName.getOrElse("")
    val s_date_field = pgc.getSourceDateField.getOrElse("")
    val t_table = pgc.getTargetTableName.getOrElse("")

    val date_max = pgc.getDateMax(spark) //pgc.getDateMax.getOrElse("2019-01-01")

    val load_type = pgc.getLoadType.getOrElse("full")
    val hash_field = pgc.getSourcePK.mkString(",")
    println("hash_field = " + hash_field)

    // load table from source
    println(s"Table ${s_table}")
    val s_df = pgc.readSource(spark, host, port, db, user, schema, s_table, s_date_field, date_max, load_type, pks)
    s_df.show()

    // write table to target
    pgc.writeSource(spark, s_df, host, port, db, user, schema, t_table, load_type, hash_field)

  }


  //@Test
  def calculateMaxDate(): Unit = {

    // create Embedded Postgres Tables
    createPostgresTables()

    val mapy = Map("S_TABLE_NAME" -> "source", "S_TABLE_TYPE" -> "postgres", "S_DATE_FIELD" -> "date_update",
      "T_TABLE_NAME" -> "target", "T_TABLE_TYPE" -> "postgres", //"T_DATE_MAX" -> "2010-10-16",
      "HOST" -> "localhost", "PORT" -> s"${pg.getEmbeddedPostgres.getPort}", "DATABASE" -> "postgres", "USER" -> "postgres",
      "SCHEMA" -> "public", "T_LOAD_TYPE" -> "scd1"
    )
    val dates = List("date_update", "date_update2", "date_update3")
    val pks = List("id", "pk2")
    val pgc = new PostgresConf(mapy, dates, pks)

    val url = f"jdbc:postgresql://${pgc.getHost.getOrElse("localhost")}:${pgc.getPort.getOrElse("5432")}/" +
      f"${pgc.getDB.getOrElse("postgres")}?user=${pgc.getUser.getOrElse("postgres")}&currentSchema=public"
    //val date_max = pgc.calculDateMax(spark, url, pgc.getTargetTableType.getOrElse("target"),
    //pgc.getTargetTableName.getOrElse("target"), pgc.getDateFields)

    val date_max = pgc.getDateMax(spark)
    println("Date Max = " + date_max)

    assert(date_max == "2019-09-25 20:16:07.0") //"2019-10-16"
  }


  /*@Test
  def test(): Unit = {
    val filename = "deltaToPg.yaml"     //"postgresToDelta.yaml"
    val ymlTxt = Source.fromFile(filename).mkString
    val yaml = ymlTxt.stripMargin.parseYaml
    val palette = yaml.convertTo[Database]

    for (pal <- palette.tables.getOrElse(Nil)) {
      println("bob" + pal.isActive.get.toString())
    }
    println(palette.toYaml.prettyPrint)

    /*val spark = SparkSession.builder()
      .appName("Spark Session")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")*/

    val pg2d2:io.frama.parisni.spark.sync.copy.PostgresToDelta2 = new io.frama.parisni.spark.sync.copy.PostgresToDelta2
    pg2d2.sync(spark, palette, pg.getEmbeddedPostgres.getPort.toString)

  }*/

  //@Test
  def sync(): Unit = {

    createPostgresTables()

    val filename = "postgresToDelta.yaml" //"deltaToPg.yaml"
    val ymlTxt = Source.fromFile(filename).mkString
    val yaml = ymlTxt.stripMargin.parseYaml
    val database = yaml.convertTo[Database]

    for (pal <- database.tables.getOrElse(Nil)) {
      println("bob" + pal.isActive.getOrElse("").toString())
    }
    println(database.toYaml.prettyPrint)
    //println(pg.getEmbeddedPostgres.getJdbcUrl("postgres", "postgres"))

    val hostPg = database.hostPg.toString
    val portPg = pg.getEmbeddedPostgres.getPort.toString //database.portPg.toString
    val databasePg = database.databasePg.toString
    val userPg = database.userPg.toString
    val dateFieldPg = database.timestampLastColumn.getOrElse("")
    val dateFieldsDelta = database.timestampColumns.getOrElse(List())
    val dateMax = database.dateMax.getOrElse("2018-10-16 23:16:16")

    for (table <- database.tables.getOrElse(Nil)) {
      //if (table.isActive.getOrElse(true)) {

      val schemaPg = table.schemaPg.toString
      val tablePg = table.tablePg.toString
      val pathDelta = table.schemaHive.toString
      val tableDelta = table.tableHive.toString
      val loadType = table.typeLoad.getOrElse("full")
      val pks = table.key

      val config = Map("S_TABLE_NAME" -> tablePg, "S_TABLE_TYPE" -> "postgres",
        "S_DATE_FIELD" -> dateFieldPg, "HOST" -> hostPg, "PORT" -> portPg,
        "DATABASE" -> databasePg, "USER" -> userPg, "SCHEMA" -> schemaPg,

        "T_TABLE_NAME" -> tableDelta, "T_TABLE_TYPE" -> "delta",
        "PATH" -> pathDelta, "T_LOAD_TYPE" -> loadType, "T_DATE_MAX" -> dateMax
      )

      val sync = new Sync()
      sync.syncSourceTarget(spark, config, dateFieldsDelta, pks)

      //}
    }
  }

}


import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  // looks like crazy but compatibility issue with junit rule (public)
  @(Rule@getter)
  var pg: SingleInstancePostgresRule = EmbeddedPostgresRules.singleInstance()

  def getPgUrl = pg.getEmbeddedPostgres.getJdbcUrl("postgres", "postgres") + "&currentSchema=public"

  def getPgTool() = PGTool(spark, getPgUrl, "/tmp")

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }


  def checkTableExists(spark: SparkSession, url: String, table: String, schema: String): Boolean = {

    val q1 = "SELECT EXISTS (SELECT FROM pg_tables " +
      f"WHERE schemaname = '${schema}' AND tablename = '${table}')"
    println(q1.toString())

    //val q = f"SELECT * FROM pg_class WHERE oid = '${schema}.${table}'"    //::regclass

    /*val q2 = "SELECT EXISTS (SELECT FROM pg_catalog.pg_class c " +
      "JOIN  pg_catalog.pg_namespace n ON n.oid = c.relnamespace " +
      f"WHERE  n.nspname = '${schema}' " +
      f"AND   c.relname = '${table}')"*/

    spark.read.format("jdbc") //postgres
      .option("url", url)
      .option("query", q1)
      .load.first.get(0).equals(true)
  }

}
