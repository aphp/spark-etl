package io.frama.parisni.spark.sync.copy

import net.jcazevedo.moultingyaml._
//import org.scalatest.FunSuite

import scala.io.Source
import SolrToSolrYaml._
class SolrToSolrTest extends SolrConfTest {

  //@Test
  def testSolr2Solr(): Unit = {

    println("io.frama.parisni.spark.sync.Sync Solr2Solr")
    // Create table "source"
    createSolrTables

    val filename = "solrToSolr.yaml"
    val ymlTxt = Source.fromFile(filename).mkString
    val yaml = ymlTxt.stripMargin.parseYaml
    val palette = yaml.convertTo[Database]

    for (pal <- palette.tables.getOrElse(Nil)) {
      println("bob" + pal.toString())
    }
    println(palette.toYaml.prettyPrint)

    println("Solr2Solr ------------------")
    //startSolrCloudCluster
    val s2s2: SolrToSolr2 = new SolrToSolr2
    s2s2.sync(spark, palette, zkHost)

  }
}
