package io.frama.parisni.spark.sync.copy

import org.apache.spark.sql.DataFrame
import java.sql.Timestamp
import net.jcazevedo.moultingyaml._
import org.junit.Test
//import org.scalatest.FunSuite

import scala.io.Source
import DeltaToSolrYaml._

class SolrToDeltaTest extends SolrConfTest {

  //@Test
  def testSolr2Delta(): Unit = {
    //println("io.frama.parisni.spark.sync.Sync Solr2Delta")

    import spark.implicits._
    // Create table "source"
    //startSolrCloudCluster
    createSolrTables

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

    val sourceDeltaTable = "/tmp/source"
    /*val dc:io.frama.parisni.spark.sync.conf.DeltaConf = new io.frama.parisni.spark.sync.conf.DeltaConf(Map("T_LOAD_TYPE" -> "full", "S_TABLE_TYPE" -> "delta", "T_TABLE_TYPE" -> "postgres"), List(""), List(""))
    dc.writeSource(spark, sInputDF, "/tmp", "source", "full")*/
    sInputDF.write
      .format("delta")
      .mode("overwrite")
      .save(sourceDeltaTable)

    val filename = "solrToDelta.yaml"
    val ymlTxt = Source.fromFile(filename).mkString
    val yaml = ymlTxt.stripMargin.parseYaml
    val palette = yaml.convertTo[Database]

    for (pal <- palette.tables.getOrElse(Nil)) {
      println("bob" + pal.toString())
    }
    println(palette.toYaml.prettyPrint)

    println("Solr2Delta ------------------")
    val solr2d2: SolrToDelta2 = new SolrToDelta2
    solr2d2.sync(spark, palette, zkHost)

  }
}
