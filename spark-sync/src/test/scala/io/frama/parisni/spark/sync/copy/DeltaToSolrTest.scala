package io.frama.parisni.spark.sync.copy

import java.sql.Timestamp

import io.frama.parisni.spark.sync.conf.DeltaConf
import io.frama.parisni.spark.sync.copy.DeltaToSolrYaml.Database
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.DataFrame

import scala.io.Source

class DeltaToSolrTest extends SolrConfTest {

  test("test delta to solr based on a max") {

    import spark.implicits._

    // Create table "source"
    val s_inputDF: DataFrame = (
      (1, "id1s", "Delta details of 1st row source", Timestamp.valueOf("2016-02-01 23:00:01"),
        Timestamp.valueOf("2016-06-16 00:00:00"), Timestamp.valueOf("2016-06-16 00:00:00")) ::
        (2, "id2s", "Delta details of 2nd row source", Timestamp.valueOf("2017-06-05 23:00:01"),
          Timestamp.valueOf("2016-06-16 00:00:00"), Timestamp.valueOf("2016-06-16 00:00:00")) ::
        (3, "id3s", "Delta details of 3rd row source", Timestamp.valueOf("2017-08-07 23:00:01"),
          Timestamp.valueOf("2016-06-16 00:00:00"), Timestamp.valueOf("2016-06-16 00:00:00")) ::
        (4, "id4s", "Delta details of 4th row source", Timestamp.valueOf("2018-10-16 23:00:01"),
          Timestamp.valueOf("2016-06-16 00:00:00"), Timestamp.valueOf("2016-06-16 00:00:00")) ::
        (5, "id5", "Delta details of 5th row source", Timestamp.valueOf("2019-12-27 00:00:00"),
          Timestamp.valueOf("2016-06-16 00:00:00"), Timestamp.valueOf("2016-06-16 00:00:00")) ::
        (6, "id6", "Delta details of 6th row source", Timestamp.valueOf("2020-01-14 00:00:00"),
          Timestamp.valueOf("2016-06-16 00:00:00"), Timestamp.valueOf("2016-06-16 00:00:00")) ::
        Nil).toDF("id", "pk2", "details", "date_update", "date_update2", "date_update3")

    val dc: DeltaConf = new DeltaConf(Map("T_LOAD_TYPE" -> "full", "S_TABLE_TYPE" -> "delta", "T_TABLE_TYPE" -> "postgres"), List(""), List(""))
    dc.writeSource(spark, s_inputDF, "/tmp", "source", "full")

    startSolrCloudCluster

    val filename = "deltaToSolr.yaml"
    val ymlTxt = Source.fromFile(filename).mkString
    val yaml = ymlTxt.stripMargin.parseYaml
    val palette = yaml.convertTo[Database]

    println("delta2Solr ------------------")
    val d2solr2: DeltaToSolr = new DeltaToSolr
    d2solr2.sync(spark, palette, zkHost)

    import com.lucidworks.spark.util.SolrDataFrameImplicits._
    val options = Map("collection" -> "target", "zkhost" -> zkHost.toString, "commit_within" -> "5000", "fields" -> "id,pk2,date_update", "request_handler" -> "/export") //, "soft_commit_secs"-> "10")
    s_inputDF.write.options(options).solr("target")


    assert(spark.read.solr("target", options).count == 2L)

  }
}
