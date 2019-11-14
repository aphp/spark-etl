package io.frama.parisni.spark.postgres

import com.typesafe.scalalogging.LazyLogging

class PostgresConf(config: Map[String, String]) extends Serializable with LazyLogging {

  val PG_NUM_PARTITION = "partitions"
  val PG_REINDEX = "reindex"
  val PG_TYPE = "type"
  var PG_HOST: String = "host"
  var PG_PORT: String = "port"
  var PG_DATABASE: String = "database"
  var PG_USER: String = "user"
  var PG_SCHEMA: String = "schema"
  var PG_TEMP: String = "temp"
  var PG_TABLE: String = "table"
  var PG_QUERY: String = "query"
  var PG_MULTILINE: String = "multiline"
  var PG_NUM_SPLITS: String = "numSplits"
  var PG_PARTITION_COLUMN: String = "partitionColumn"
  var PG_JOIN_KEY: String = "joinkey"
  var PG_PASSWORD: String = "password"
  var PG_URL: String = "url"
  var PG_END_COLUMN: String = "endCol"
  var PG_PK: String = "pk"

  require(config != null, "Config cannot be null")
  require(config.nonEmpty, "Config cannot be empty")

  require(config.get(PG_TYPE).isEmpty || (config.get(PG_TYPE).isDefined
    && ("full" :: "scd1" :: "scd2" :: Nil).contains(config.get(PG_TYPE).get)), "type shall be in full, scd1")

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

}

