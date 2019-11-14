package postgres

import com.typesafe.scalalogging.LazyLogging
import io.frama.parisni.spark.dataframe.DFTool
import io.frama.parisni.spark.postgres.{PGTool, PostgresConf}
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
      val solrRelation: PostgresRelation = new PostgresRelation(parameters, Some(df), sqlContext.sparkSession)
      solrRelation.insert(df, overwrite = mode.name().toLowerCase() == "overwrite")
      solrRelation
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

    if (overwrite)
      _pg.tableDrop(table)
    _pg.tableCreate(table, data.schema, false)

    loadType match {
      case "full" => _pg.outputBulk(table, data, numPartitions.get, reindex)
      case "scd1" => _pg.outputScd1Hash(table, joinKey.get.toList, DFTool.dfAddHash(data), numPartitions)
      case "scd2" => _pg.outputScd2Hash(table, DFTool.dfAddHash(data), pk.get, joinKey.get.toList, endCol.get, numPartitions)
    }
    _pg.purgeTmp()
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

  def getPool() = {

    require(conf.getHost.nonEmpty || conf.getUrl.isDefined, "Host cannot be empty")
    require(conf.getDatabase.nonEmpty || conf.getUrl.isDefined, "Database cannot be empty")
    require(conf.getUser.nonEmpty || conf.getUrl.isDefined, "User cannot be empty")

    val url = getUrl(conf.getUrl, conf.getHost, conf.getPort, conf.getDatabase, conf.getUser, conf.getSchema)
    pgTool(url, conf.getTemp, conf.getPassword)
  }

  def pgTool(url: String, tempFolder: Option[String], password: Option[String]) = {
    val pg = PGTool(sparkSession, url, tempFolder.getOrElse("/tmp"))
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
