package io.frama.parisni.spark.postgres

import com.typesafe.scalalogging.LazyLogging

class PostgresConf(config: Map[String, String]) extends Serializable with LazyLogging {

  val PG_NUM_PARTITION = "partitions"
  val PG_REINDEX = "reindex"
  val PG_TYPE = "type"
  val PG_HOST: String = "host"
  val PG_PORT: String = "port"
  val PG_DATABASE: String = "database"
  val PG_USER: String = "user"
  val PG_SCHEMA: String = "schema"
  val PG_TEMP: String = "temp"
  val PG_TABLE: String = "table"
  val PG_QUERY: String = "query"
  val PG_MULTILINE: String = "multiline"
  val PG_NUM_SPLITS: String = "numSplits"
  val PG_PARTITION_COLUMN: String = "partitionColumn"
  val PG_JOIN_KEY: String = "joinkey"
  val PG_PASSWORD: String = "password"
  val PG_URL: String = "url"
  val PG_END_COLUMN: String = "endCol"
  val PG_PK: String = "pk"
  val PG_FILTER: String = "filter"
  val PG_DELETE_SET: String = "deleteSet"
  val PG_KILL_LOCKS: String = "kill-locks"
  val PG_SWAP_LOAD: String = "swap-load"
  val PG_BULK_LOAD_MODE: String = "bulkLoadMode"

  require(config != null, "Config cannot be null")
  require(config.nonEmpty, "Config cannot be empty")

  require(config.get(PG_TYPE).isEmpty || (config.get(PG_TYPE).isDefined
    && ("full" :: "megafull" :: "scd1" :: "scd2" :: Nil).contains(config.get(PG_TYPE).get)), "type shall be in full, scd1")

  def getQuery: Option[String] = config.get(PG_QUERY)

  def getUrl: Option[String] = config.get(PG_URL)

  def getPassword: Option[String] = config.get(PG_PASSWORD)


  def getPrimaryKey: Option[String] = config.get(PG_PK)

  def getJoinKey: Option[Array[String]] =
    if (config.get(PG_JOIN_KEY).isDefined) Some(config(PG_JOIN_KEY).split(",")) else None

  def getPartitionColumn: Option[String] = config.get(PG_PARTITION_COLUMN)

  def getEndColumn: Option[String] = config.get(PG_END_COLUMN)

  def getIsMultiline: Option[Boolean] =
    if (config.get(PG_MULTILINE).isDefined) Some(config(PG_MULTILINE).toBoolean) else Some(false)

  def getNumSplits: Option[Int] =
    if (config.get(PG_NUM_SPLITS).isDefined) Some(config(PG_NUM_SPLITS).toInt) else Some(4)

  def getTemp: Option[String] = config.get(PG_TEMP)

  def getHost: Option[String] = config.get(PG_HOST)

  def getPort: Option[String] = config.get(PG_PORT)

  def getDatabase: Option[String] = config.get(PG_DATABASE)

  def getUser: Option[String] = config.get(PG_USER)

  def getSchema: Option[String] = config.get(PG_SCHEMA)

  def getTable: Option[String] = config.get(PG_TABLE)

  def getType: Option[String] = config.get(PG_TYPE)

  def getNumPartition: Option[Int] =
    if (config.get(PG_NUM_PARTITION).isDefined) Some(config(PG_NUM_PARTITION).toInt) else Some(1)

  def getIsReindex: Option[Boolean] =
    if (config.get(PG_REINDEX).isDefined) Some(config(PG_REINDEX).toBoolean) else Some(false)

  def getFilter: Option[String] = config.get(PG_FILTER)

  def getDeleteSet: Option[String] = config.get(PG_DELETE_SET)

  def getKillLocks: Option[Boolean] =
    if (config.get(PG_KILL_LOCKS).isDefined) Some(config(PG_KILL_LOCKS).toBoolean) else Some(false)

  def getSwapLoad: Option[Boolean] =
    if (config.get(PG_SWAP_LOAD).isDefined) Some(config(PG_SWAP_LOAD).toBoolean) else Some(false)

  def getBulkLoadMode: Option[String] = config.get(PG_BULK_LOAD_MODE)

}

