package io.frama.parisni.spark.sync.copy

import java.sql.Timestamp

import io.frama.parisni.spark.sync.conf.PostgresConf
import io.frama.parisni.spark.sync.copy.PostgresToPgYaml.Database
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.DataFrame
//import org.junit.Test

import scala.io.Source

class PostgresToPgTest extends SolrConfTest { //FunSuite with io.frama.parisni.spark.sync.copy.SparkSessionTestWrapper

  //test("test PG to PG") {

  //@Test
  def testPg2Pg(): Unit = {

    import spark.implicits._
    println("io.frama.parisni.spark.sync.Sync Postgres To Postgres")
    //val url = f"jdbc:postgresql://localhost:${pg.getEmbeddedPostgres.getPort}/postgres?user=postgres&currentSchema=public"

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

    /*sInputDF.write.format("postgres")
      .option("url", url)
      .option("table", "source")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save*/

    val pgc: PostgresConf = new PostgresConf(
      Map(
        "T_LOAD_TYPE" -> "full",
        "S_TABLE_TYPE" -> "postgres",
        "T_TABLE_TYPE" -> "postgres"
      ),
      List(""),
      List("")
    )
    pgc.writeSource(
      spark,
      sInputDF,
      "localhost",
      pg.getEmbeddedPostgres.getPort.toString,
      "postgres",
      "postgres",
      "public",
      "source",
      "full",
      "hash"
    )

    val filename = "postgresToPg.yaml"
    val ymlTxt = Source.fromFile(filename).mkString
    val yaml = ymlTxt.stripMargin.parseYaml
    val palette = yaml.convertTo[Database]

    for (pal <- palette.tables.getOrElse(Nil)) {
      println("bob" + pal.toString())
    }
    println(palette.toYaml.prettyPrint)

    println("Pg2Pg ------------------")
    val pg2pg2: PostgresToPg2 = new PostgresToPg2
    pg2pg2.sync(spark, palette, pg.getEmbeddedPostgres.getPort.toString)

  }
}
