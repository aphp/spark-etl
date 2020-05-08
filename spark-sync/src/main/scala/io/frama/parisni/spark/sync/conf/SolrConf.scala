package io.frama.parisni.spark.sync.conf

import java.util
import java.util.Optional

import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.request.QueryRequest
import org.apache.solr.common.params.{CollectionParams, CoreAdminParams, ModifiableSolrParams}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class SolrConf(config: Map[String, String], dates: List[String], pks: List[String])
  extends SourceAndTarget { //io.frama.parisni.spark.sync.conf.TargetConf with io.frama.parisni.spark.sync.conf.SourceConf with LazyLogging{

  checkTargetParams(config)
  checkSourceParams(config)

  // SourceTable fields & methods
  val ZKHOST: String = "ZKHOST" // ZKHOST & Collection are used by Solr for connexion
  def getZkHost: Option[String] = config.get(ZKHOST)

  def readSource(spark: SparkSession, zkhost: String, collection: String, //cloudClient: CloudSolrClient,
                 s_date_field: String, date_Max: String, load_type: String = "full"): DataFrame = {

    import org.apache.spark.sql.functions._
    try {
      logger.warn("Reading data from Solr collection---------")
      var dfSolr = spark.emptyDataFrame

      if (!checkCollectionExists(collection, zkhost)) {
        logger.warn(s"Collection ${collection} doesn't exist !!")
        return spark.emptyDataFrame
      }

      val options = Map("collection" -> collection, "zkhost" -> zkhost)
      dfSolr = spark.read.format("solr").options(options).load
      logger.warn("Full Solr DataFrame")
      dfSolr.show()

      if (load_type != "full")
        dfSolr = dfSolr.filter(f"${s_date_field} >= '${date_Max}'")

      //change "id" type to Integer (Solr uses "id" as String)
      //val dfSolr2 = dfSolr.selectExpr("cast(id as int) id", "make")
      val dfSolr2 = dfSolr.withColumn("id", col("id").cast(IntegerType))

      logger.warn("Solr DataFrame after filter DateMax")
      dfSolr2
    } catch {
      case re: RuntimeException => throw re
      case e: Exception => throw new RuntimeException(e)
    }
  }

  override def getSourceTableName = config.get(S_TABLE_NAME)

  override def getSourceTableType = config.get(S_TABLE_TYPE)

  override def getSourceDateField = config.get(S_DATE_FIELD)

  def getSourcePK = pks

  // TargetTable methods
  override def getTargetTableName = config.get(T_TABLE_NAME)

  override def getTargetTableType = config.get(T_TABLE_TYPE)

  override def getLoadType = config.get(T_LOAD_TYPE)

  def getDateFields = dates

  override def getDateMax(spark: SparkSession): String =
    if (config.get(T_DATE_MAX).isDefined) config.get(T_DATE_MAX).getOrElse("")
    else if (!checkCollectionExists(getTargetTableName.getOrElse(""), getZkHost.getOrElse(""))) {
      "1900-01-01 00:00:00"
    }
    else
      calculDateMax(spark, getZkHost.getOrElse(""), getTargetTableType.getOrElse(""), getTargetTableName.getOrElse(""), getDateFields)

  // Write to Solr Collection
  def writeSource(s_df: DataFrame, zkhost: String, collection: String, load_type: String = "full"): Unit = {

    try {
      logger.warn("Writing data into Solr collection---------")

      // Initiate a cloud solr client
      implicit val solrClient: CloudSolrClient =
        new CloudSolrClient.Builder(util.Arrays.asList(zkhost), Optional.empty()).build

      if (!checkCollectionExists(collection, zkhost)) {
        logger.warn(s"Creating solr collection ${collection} from scratch")

        val modParams = new ModifiableSolrParams()
        modParams.set(CoreAdminParams.ACTION, CollectionParams.CollectionAction.CREATE.name)
        modParams.set("name", collection)
        modParams.set("numShards", 1)
        //modParams.set("replicationFactor", replicationFactor)
        //modParams.set("maxShardsPerNode", maxShardsPerNode)
        //modParams.set("collection.configName", confName)
        val request: QueryRequest = new QueryRequest(modParams)
        request.setPath("/admin/collections")
        solrClient.request(request)

        //Explicit commit
        solrClient.commit(collection)
      }

      val options = Map("collection" -> collection, "zkhost" -> zkhost, "commit_within" -> "5000") //, "soft_commit_secs"-> "10")
      s_df.write.format("solr")
        .options(options)
        .mode(SaveMode.Overwrite)
        .save

    } catch {
      case re: RuntimeException => throw re
      case e: Exception => throw new RuntimeException(e)
    }
  }

  def checkCollectionExists(collection: String, zkhost: String): Boolean = {

    implicit val solrClient: CloudSolrClient =
      new CloudSolrClient.Builder(util.Arrays.asList(zkhost), Optional.empty()).build

    solrClient.getZkStateReader.getClusterState.hasCollection(collection)
  }
}
