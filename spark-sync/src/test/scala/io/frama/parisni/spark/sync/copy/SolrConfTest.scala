package io.frama.parisni.spark.sync.copy

import java.io.File
import java.sql.Timestamp
import java.util
import java.util.Optional

import com.lucidworks.spark.util.SolrSupport
import io.frama.parisni.spark.sync.conf.SolrConf
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.cloud.MiniSolrCloudCluster
import org.apache.solr.common.cloud.ZkConfigManager
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.eclipse.jetty.servlet.ServletHolder
import org.junit.Assert.assertTrue
import org.restlet.ext.servlet.ServerServlet

class SolrConfTest
    extends QueryTest
    with SparkSessionTestWrapper
    with SolrCloudTestBuilder {

  test("test collection exists") {

    startSolrCloudCluster()
    println(
      "Before --- Collection source exists= " + checkCollectionExists(
        "source",
        zkHost
      )
    )
    println(
      "Before --- Collection target exists= " + checkCollectionExists(
        "target",
        zkHost
      )
    )
    println(
      "Before --- Collection source55 exists= " + checkCollectionExists(
        "source55",
        zkHost
      )
    )

    createSolrTables()

    println(
      "After --- Collection source exists= " + checkCollectionExists(
        "source",
        zkHost
      )
    )
    println(
      "After --- Collection target exists= " + checkCollectionExists(
        "target",
        zkHost
      )
    )
    println(
      "Before --- Collection source55 exists= " + checkCollectionExists(
        "source55",
        zkHost
      )
    )
  }

  test("test sync solr to solr") {

    // create Embedded Delta Tables
    createSolrTables()
    val clusterState = cloudClient.getZkStateReader.getClusterState
    val solrCloudClient = SolrSupport.getCachedCloudClient(zkHost)

    val mapy = Map(
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
    val solrc = new SolrConf(mapy, dates, pks)

    val zkhost = solrc.getZkHost.getOrElse("")
    val sCollection = solrc.getSourceTableName.getOrElse("")
    val sDateField = solrc.getSourceDateField.getOrElse("")
    val tCcollection = solrc.getTargetTableName.getOrElse("")
    val dateMax =
      solrc.getDateMax(spark) //pgc.getDateMax.getOrElse("2019-01-01")

    // load collection from source
    println(s"Collection ${sCollection}")
    val sDf = solrc.readSource(spark, zkhost, sCollection, sDateField, dateMax)
    sDf.show()

    // write collection to target
    if (!solrc.checkCollectionExists(tCcollection, zkhost)) //solrCloudClient,
      SolrCloudUtil.buildCollection(
        zkHost,
        tCcollection,
        null,
        1,
        cloudClient,
        spark.sparkContext
      )
    solrc.writeSource(sDf, zkhost, tCcollection) //cloudClient,

    solrCloudClient.commit(tCcollection, true, true)

    //show target collection after update
    solrc.readSource(spark, zkhost, tCcollection, sDateField, dateMax).show()
  }

  def startSolrCloudCluster(): Unit = {

    System.setProperty("jetty.testMode", "true")
    val solrXml = new File("src/test/resources/solr.xml")
    val solrXmlContents: String =
      TestSolrCloudClusterSupport.readSolrXml(solrXml)

    val targetDir = new File("target")
    if (!targetDir.isDirectory)
      fail(
        "Project 'target' directory not found at :" + targetDir.getAbsolutePath
      )

    testWorkingDir =
      new File(targetDir, "scala-solrcloud-" + System.currentTimeMillis)
    if (!testWorkingDir.isDirectory)
      testWorkingDir.mkdirs

    // need the schema stuff
    val extraServlets: java.util.SortedMap[ServletHolder, String] =
      new java.util.TreeMap[ServletHolder, String]()

    val solrSchemaRestApi: ServletHolder =
      new ServletHolder("SolrSchemaRestApi", classOf[ServerServlet])
    solrSchemaRestApi.setInitParameter(
      "org.restlet.application",
      "org.apache.solr.rest.SolrSchemaRestApi"
    )
    extraServlets.put(solrSchemaRestApi, "/schema/*") //delete \ before *

    cluster = new MiniSolrCloudCluster(
      1,
      null /* hostContext */,
      testWorkingDir.toPath(),
      solrXmlContents,
      extraServlets,
      null
    )
    cloudClient = cluster.getSolrClient
    cloudClient.connect()
    //println("cloudClient = "+cloudClient.toString)

    assertTrue(
      !cloudClient.getZkStateReader.getClusterState.getLiveNodes.isEmpty
    )
    zkHost = cluster.getZkServer.getZkAddress
    println("zkHost = " + zkHost)

    /*val clusterState = cloudClient.getZkStateReader.getClusterState
    val colState = clusterState.getCollectionOrNull(collection)*/

    // skClient config
    val zkClient = cloudClient.getZkStateReader.getZkClient
    val zkConfigManager = new ZkConfigManager(zkClient)
    val confName = "testConfig"
    val confDir = new File("src/test/resources/conf")
    zkConfigManager.uploadConfigDir(confDir.toPath, confName)

  }

  def createSolrTables(): Unit = { // Uses JUnit-style assertions

    import spark.implicits._

    startSolrCloudCluster()
    val solrCloudClient = SolrSupport.getCachedCloudClient(zkHost)

    // Create table "source"
    val sInputDF: DataFrame = ((
      1,
      "id1s",
      "Solr details of 1st row source",
      Timestamp.valueOf("2016-02-01 23:00:01"),
      Timestamp.valueOf("2016-06-16 00:00:00"),
      Timestamp.valueOf("2016-06-16 00:00:00")
    ) ::
      (
        2,
        "id2s",
        "Solr details of 2nd row source",
        Timestamp.valueOf("2017-06-05 23:00:01"),
        Timestamp.valueOf("2016-06-16 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00")
      ) ::
      (
        3,
        "id3s",
        "Solr details of 3rd row source",
        Timestamp.valueOf("2017-08-07 23:00:01"),
        Timestamp.valueOf("2016-06-16 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00")
      ) ::
      (
        4,
        "id4s",
        "Solr details of 4th row source",
        Timestamp.valueOf("2018-10-16 23:00:01"),
        Timestamp.valueOf("2016-06-16 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00")
      ) ::
      (
        5,
        "id5",
        "Solr details of 5th row source",
        Timestamp.valueOf("2019-12-27 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00"),
        Timestamp.valueOf("2016-06-16 00:00:00")
      ) ::
      (
        6,
        "id6",
        "Solr details of 6th row source",
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

    val sCollectionName = "source"
    SolrCloudUtil.buildCollection(
      zkHost,
      sCollectionName,
      null,
      1,
      cloudClient,
      spark.sparkContext
    )
    val sSolrOpts = Map(
      "zkhost" -> zkHost,
      "collection" -> sCollectionName,
      "flatten_multivalued" -> "false" // for correct reading column "date"
    )
    sInputDF.write.format("solr").options(sSolrOpts).mode(Overwrite).save()

    // Explicit commit to make sure all docs are visible
    solrCloudClient.commit(sCollectionName, true, true)

    val sOutputDF = spark.read.format("solr").options(sSolrOpts).load
    println("Source table: ")
    sOutputDF.show()

  }

  def checkCollectionExists(collection: String, zkhost: String): Boolean = { //private   solrClient: CloudSolrClient,

    implicit val solrClient: CloudSolrClient =
      new CloudSolrClient.Builder(
        util.Arrays.asList(zkhost),
        Optional.empty()
      ).build

    solrClient.getZkStateReader.getClusterState.hasCollection(collection)
  }
}
