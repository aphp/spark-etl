package io.frama.parisni.spark.postgres

import java.util.UUID._

import com.typesafe.scalalogging.LazyLogging
import io.frama.parisni.spark.dataframe.DFTool
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

class DefaultSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister with LazyLogging {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    try {
      new PostgresRelation(parameters, None, sqlContext.sparkSession)

    } catch {
      case re: RuntimeException => throw re
      case e: Exception => throw new RuntimeException(e)
    }
  }

  override def createRelation(
                               sqlContext: SQLContext,
                               mode: SaveMode,
                               parameters: Map[String, String],
                               df: DataFrame): BaseRelation = {
    try {
      // TODO: What to do with the saveMode?
      val postgresRelation = new PostgresRelation(parameters, Some(df), sqlContext.sparkSession)
      postgresRelation.insert(df, overwrite = mode.name().toLowerCase() == "overwrite")
      postgresRelation
    } catch {
      case re: RuntimeException => throw re
      case e: Exception => throw new RuntimeException(e)
    }
  }

  override def shortName(): String = "postgres"
  
}

class PostgresRelation(val parameters: Map[String, String]
                       , val dataFrame: Option[DataFrame]
                       , @transient val sparkSession: SparkSession)(
                        implicit val conf: PostgresConf = new PostgresConf(parameters)
                      ) extends BaseRelation
  with Serializable
  with InsertableRelation
  with TableScan
  with LazyLogging {

  val _pg = getPool

  override val sqlContext: SQLContext = sparkSession.sqlContext

  private def loadByType(
                         loadType: String
                         , tableToLoad: String
                         , data: DataFrame
                         , numPartitions: Option[Int]
                         , reindex: Boolean
                         , joinKey: Option[Seq[String]] = None
                         , filter: Option[String] = None
                         , deleteSet: Option[String] = None
                         , pk: Option[String] = None
                         , endCol: Option[String] = None
                        ): Unit = {
    loadType match {
      case "full" => _pg.outputBulk(tableToLoad, data, numPartitions, reindex)
      case "megafull" => _pg.outputBulk(tableToLoad, data, numPartitions, reindex)
      case "scd1" => _pg.outputScd1Hash(tableToLoad, joinKey.get, DFTool.dfAddHash(data), numPartitions, filter, deleteSet)
      case "scd2" => _pg.outputScd2Hash(tableToLoad, DFTool.dfAddHash(data), pk.get, joinKey.get, endCol.get, numPartitions)
    }
  }

  private def swapLoad(
                       table : String
                       , killLocks: Boolean
                       , loadType: String
                       , data: DataFrame
                       , numPartitions: Option[Int]
                       , reindex: Boolean
                       , joinKey: Option[Seq[String]] = None
                       , filter: Option[String] = None
                       , deleteSet: Option[String] = None
                       , pk: Option[String] = None
                       , endCol: Option[String] = None
                      ): Unit = {
    logger.warn("overwrite + swap loading")

    // Overwrite-Swap bulk-loading strategy:
    // Copy a temporary table  frgom the original table,
    // Bulk-load the data into it and when done
    // drop existing and rename newly loaded
    val tmpTable = "table_" + randomUUID.toString.replaceAll(".*-", "")
    _pg.tableCopy(table
                  , tmpTable
                  , isUnlogged = false
                  , copyConstraints = true
                  , copyIndexes = true
                  , copyStorage = true
                  , copyComments = true
                  , copyOwner = true
                  , copyPermissions = true
    )

    loadByType(loadType, tmpTable, data, numPartitions, reindex, joinKey, filter, deleteSet, pk, endCol)

    _pg.purgeTmp()

    if (killLocks)
      _pg.killLocks(table)
    _pg.tableDrop(table)
    _pg.tableRename(tmpTable, table)
  }

  private def inPlaceLoad(
                          table : String
                          , overwrite: Boolean
                          , killLocks: Boolean
                          , loadType: String
                          , data: DataFrame
                          , numPartitions: Option[Int]
                          , reindex: Boolean
                          , joinKey: Option[Seq[String]] = None
                          , filter: Option[String] = None
                          , deleteSet: Option[String] = None
                          , pk: Option[String] = None
                          , endCol: Option[String] = None
                         ): Unit = {
    logger.warn("in-place loading")
    logger.warn("is_overwrite " + overwrite)

    // Kill locks before dropping in case of overwrite
    if (overwrite) {
      if (killLocks)
        _pg.killLocks(table)
      _pg.tableDrop(table)
    }

    // Create table in any case: whether dropped because of overwrite, new table, or existing (noop)
    _pg.tableCreate(table, data.schema, isUnlogged = false)

    // Kill locks just before loading if not overwrite
    if ((!overwrite) && killLocks)
      _pg.killLocks(table)

    loadByType(loadType, table, data, numPartitions, reindex, joinKey, filter, deleteSet, pk, endCol)

    _pg.purgeTmp()

  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    require(conf.getTable.nonEmpty, "Table cannot be empty")
    val loadType = conf.getType.getOrElse("full")
    loadType match {
      case "scd1" => require(conf.getJoinKey.nonEmpty, "JoinKey cannot be empty when scd1")
      case "scd2" => require(conf.getEndColumn.nonEmpty && conf.getPrimaryKey.nonEmpty, "pk and endCol cannot be empty when scd2")
      case _ =>
    }

    val table = conf.getTable.get
    val reindex = conf.getIsReindex.get
    val numPartitions = conf.getNumPartition
    val joinKey = conf.getJoinKey
    val endCol = conf.getEndColumn
    val pk = conf.getPrimaryKey
    val filter = conf.getFilter
    val deleteSet = conf.getDeleteSet
    val killLocks = conf.getKillLocks.get
    val swapLoadParam = conf.getSwapLoad.get

    // Swap can only be applied if the original table exists
    if (overwrite && swapLoadParam && _pg.tableExists(table)) {
      swapLoad(table, killLocks, loadType, data, numPartitions, reindex, joinKey, filter, deleteSet, pk, endCol)
    } else {
      inPlaceLoad(table, overwrite, killLocks, loadType, data, numPartitions, reindex, joinKey, filter, deleteSet, pk, endCol)
    }
  }

  // https://michalsenkyr.github.io/2017/02/spark-sql_datasource
  override def buildScan(): RDD[Row] = {
    require(conf.getQuery.nonEmpty, "Query cannot be empty")
    require(conf.getNumPartition.get == 1 || conf.getPartitionColumn.isDefined
      , "For multiple partition, a partition column shall be specified")

    val query = conf.getQuery.get
    val multiline = conf.getIsMultiline
    val numPartitions = conf.getNumPartition
    val splits = conf.getNumSplits
    val partitionColumn = conf.getPartitionColumn.getOrElse("")

    val res = _pg.inputBulk(query, multiline, numPartitions, splits, partitionColumn).rdd
    _pg.purgeTmp()
    res
  }

  def getPool: PGTool = {

    require(conf.getHost.nonEmpty || conf.getUrl.isDefined, "Host cannot be empty")
    require(conf.getDatabase.nonEmpty || conf.getUrl.isDefined, "Database cannot be empty")
    require(conf.getUser.nonEmpty || conf.getUrl.isDefined, "User cannot be empty")
    val bulkLoadMode = conf.getBulkLoadMode.getOrElse("") match {
      case "csv" => CSV
      case "stream" => Stream
      case "PgBinaryStream" => PgBinaryStream
      case "PgBinaryFiles" => PgBinaryFiles
      case _ => PGTool.defaultBulkLoadStrategy
    }
    val bulkLoadBufferSize = conf.getBulkLoadBufferSize.getOrElse(PGTool.defaultBulkLoadBufferSize)

    val url = getUrl(conf.getUrl, conf.getHost, conf.getPort, conf.getDatabase, conf.getUser, conf.getSchema)
    pgTool(url, conf.getTemp, conf.getPassword, bulkLoadMode, bulkLoadBufferSize)
  }

  def pgTool(
             url: String
             , tempFolder: Option[String]
             , password: Option[String]
             , bulkLoadMode: BulkLoadMode
             , bulkLoadBufferSize: Int
            ) = {
    val pg = PGTool(sparkSession, url, tempFolder.getOrElse("/tmp"), bulkLoadMode, bulkLoadBufferSize)
    if (password.isDefined)
      pg.setPassword(password.get)
    pg
  }

  def getUrl(url: Option[String]
             , host: Option[String]
             , port: Option[String]
             , database: Option[String]
             , user: Option[String]
             , schema: Option[String]
            ) = {
    url match {
      case Some(s) => s
      case None => "jdbc:postgresql://%s:%s/%s?user=%s&currentSchema=%s"
        .format(host.get, port.getOrElse(5432), database.get, user.get, schema.getOrElse("public"))
    }

  }

  lazy val querySchema: StructType = {
    if (dataFrame.isDefined) dataFrame.get.schema
    else {
      if (conf.getQuery.isEmpty)
        throw new RuntimeException("Query shall be defined")
      val query = conf.getQuery.get
      _pg.getSchemaQuery(query)
    }
  }

  override def schema: StructType = querySchema

}
