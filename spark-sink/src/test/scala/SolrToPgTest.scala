import java.sql.Timestamp

import PostgresToSolrYaml.Database
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.DataFrame
import org.junit.Test
//import org.junit.Test

import scala.io.Source

class SolrToPgTest extends SolrConfTest{    //FunSuite with SparkSessionTestWrapper

    //test("test Solr to Postgres") {

  //@Test
  def testSolr2Pg(): Unit = {

    import spark.implicits._
    println("Sync Solr To Postgres")

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
    val solr2pg2:SolrToPg2 = new SolrToPg2
    solr2pg2.sync(spark, palette, pg.getEmbeddedPostgres.getPort.toString, zkHost)

  }
}