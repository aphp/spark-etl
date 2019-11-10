package postgres

import com.typesafe.scalalogging.LazyLogging
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
    val table = conf.getTable.get
    val reindex = conf.getIsReindex.get
    val numPartitions = conf.getNumPartition.get
    val loadType = conf.getType.getOrElse("full")

    if (overwrite)
      _pg.tableDrop(table)
    _pg.tableCreate(table, data.schema, false)

    loadType match {
      case "full" => _pg.outputBulk(table, data, numPartitions, reindex)
      case "scd1" => throw new Exception("scd1 not yet implemented")
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

    require(conf.getHost.nonEmpty, "Host cannot be empty")
    require(conf.getDatabase.nonEmpty, "Database cannot be empty")
    require(conf.getUser.nonEmpty, "User cannot be empty")

    val tempFolder = conf.getTemp.getOrElse("/tmp")
    val port = conf.getPort.getOrElse("5432")
    val schema = conf.getSchema.getOrElse("public")
    val host = conf.getHost.get
    val database = conf.getDatabase.get
    val user = conf.getUser.get
    val url = f"jdbc:postgresql://${host}:${port}/${database}?user=${user}&currentSchema=${schema}"
    logger.warn(s"postgres with url: $url")
    PGTool(sparkSession, url, tempFolder)
  }

  lazy val querySchema: StructType = {
    if (dataFrame.isDefined) dataFrame.get.schema
    else {
      val query = conf.getQuery.get
      _pg.getSchemaQuery(query)
    }
  }

  override def schema: StructType = querySchema

}
