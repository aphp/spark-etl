package io.frama.parisni.spark.sync.copy

import java.sql.Timestamp
import io.frama.parisni.spark.sync.conf.DeltaConf
import io.frama.parisni.spark.sync.copy.PostgresToDeltaYaml.Database
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.DataFrame
import org.scalatest.funsuite.FunSuite

import scala.io.Source

class DeltaToPgTest extends FunSuite with SparkSessionTestWrapper {

  //@Test
  def testDelta2Pg(): Unit = {
    //println("io.frama.parisni.spark.sync.Sync Delta2Pg")
    //val url = f"jdbc:postgresql://localhost:${pg.getEmbeddedPostgres.getPort}/postgres?user=postgres&currentSchema=public"
    //val url = getPgUrl

    import spark.implicits._

    // Create table "source"
    val sInputDF: DataFrame = ((
      1,
      "id1s",
      "Delta details of 1st row source",
      Timestamp.valueOf("2016-02-01 23:00:01"),
      Timestamp.valueOf("2016-06-16 00:00:00"),
      Timestamp.valueOf("2016-06-16 00:00:00")
    ) ::
      (
        2,
        "id2s",
        "Delta details of 2nd row source",
        Timestamp.valueOf("2017-06-05 23:00:01"),
        Timestamp.valueOf("2016-06-16 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00")
      ) ::
      (
        3,
        "id3s",
        "Delta details of 3rd row source",
        Timestamp.valueOf("2017-08-07 23:00:01"),
        Timestamp.valueOf("2016-06-16 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00")
      ) ::
      (
        4,
        "id4s",
        "Delta details of 4th row source",
        Timestamp.valueOf("2018-10-16 23:00:01"),
        Timestamp.valueOf("2016-06-16 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00")
      ) ::
      (
        5,
        "id5",
        "Delta details of 5th row source",
        Timestamp.valueOf("2019-12-27 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00")
      ) ::
      (
        6,
        "id6",
        "Delta details of 6th row source",
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

    val dc: DeltaConf = new DeltaConf(
      Map(
        "T_LOAD_TYPE" -> "full",
        "S_TABLE_TYPE" -> "delta",
        "T_TABLE_TYPE" -> "postgres"
      ),
      List(""),
      List("")
    )
    dc.writeSource(spark, sInputDF, "/tmp", "source10", "full")
    /* val sourceDeltaTable = "/tmp/source"
	sInputDF.write.format("delta")
      .mode("overwrite")
      .save(sourceDeltaTable)*/

    val filename = "deltaToPg.yaml"
    val ymlTxt = Source.fromFile(filename).mkString
    val yaml = ymlTxt.stripMargin.parseYaml
    val palette = yaml.convertTo[Database]

    for (pal <- palette.tables.getOrElse(Nil)) {
      println("bob" + pal.toString())
    }
    println(palette.toYaml.prettyPrint)

    println("delta2Pg ------------------")
    val d2pg2: DeltaToPg2 = new DeltaToPg2
    d2pg2.sync(spark, palette, pg.getEmbeddedPostgres.getPort.toString)

  }
}
