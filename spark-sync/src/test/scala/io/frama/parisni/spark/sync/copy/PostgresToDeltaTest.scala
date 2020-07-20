package io.frama.parisni.spark.sync.copy

import java.sql.Timestamp

import io.frama.parisni.spark.sync.conf.PostgresConf
import io.frama.parisni.spark.sync.copy.PostgresToDeltaYaml.Database
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.DataFrame
import org.junit.Test
import org.scalatest.FunSuite
//import org.junit.Test

import scala.io.Source

class PostgresToDeltaTest extends FunSuite with SparkSessionTestWrapper {

  //@Test
  def postgresToDeltaTest = {
    import spark.implicits._
    //val url = getPgUrl

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

    val pgc: PostgresConf = new PostgresConf(
      Map(
        "T_LOAD_TYPE" -> "full",
        "S_TABLE_TYPE" -> "postgres",
        "T_TABLE_TYPE" -> "delta"
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

    val filename = "postgresToDelta.yaml"
    val ymlTxt = Source.fromFile(filename).mkString
    val yaml = ymlTxt.stripMargin.parseYaml
    val palette = yaml.convertTo[Database]

    for (pal <- palette.tables.getOrElse(Nil)) {
      println("bob" + pal.toString())
    }
    println(palette.toYaml.prettyPrint)

    println("Pg2Delta ------------------")
    val pg2d = new PostgresToDelta
    pg2d.sync(spark, palette, pg.getEmbeddedPostgres.getPort.toString)

  }

}
