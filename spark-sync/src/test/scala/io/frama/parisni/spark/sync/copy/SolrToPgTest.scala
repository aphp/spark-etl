package io.frama.parisni.spark.sync.copy

import net.jcazevedo.moultingyaml._
//import org.junit.Test

import scala.io.Source
import PostgresToSolrYaml._

class SolrToPgTest extends SolrConfTest { //FunSuite with io.frama.parisni.spark.sync.copy.SparkSessionTestWrapper

  //test("test Solr to Postgres") {

  //@Test
  def testSolr2Pg(): Unit = {
    println("io.frama.parisni.spark.sync.Sync Solr To Postgres")

    // Create table "source"
    createSolrTables

    val filename = "solrToPg.yaml"
    val ymlTxt = Source.fromFile(filename).mkString
    val yaml = ymlTxt.stripMargin.parseYaml
    val palette = yaml.convertTo[Database]

    for (pal <- palette.tables.getOrElse(Nil)) {
      println("bob" + pal.toString())
    }
    println(palette.toYaml.prettyPrint)

    println("Solr2Pg ------------------")
    val solr2pg2: SolrToPg2 = new SolrToPg2
    solr2pg2.sync(
      spark,
      palette,
      pg.getEmbeddedPostgres.getPort.toString,
      zkHost
    )

  }
}
