package io.frama.parisni.spark.sync.conf

trait SourceConf {

  //val config: Map[String, String]

  val S_TABLE_NAME: String = "S_TABLE_NAME"
  val S_TABLE_TYPE: String = "S_TABLE_TYPE" //"postgres", "solr", "delta", ....
  val S_DATE_FIELD: String = "S_DATE_FIELD"
  //val S_PK: String = "S_PK"                 //list of PKs as: pk1,pk2,pk3,....,pkn

  //def readSource(spark: SparkSession, strings: String*): Any
  def getSourceTableName: Option[String]

  def getSourceTableType: Option[String]

  def getSourceDateField: Option[String]

  //def getSourcePK: Option[String]

  def checkSourceParams(config: Map[String, String]) = {

    require(config != null, "Config cannot be null")
    require(config.nonEmpty, "Config cannot be empty")


    require(config.get(S_TABLE_TYPE).isEmpty || (config.get(S_TABLE_TYPE).isDefined
      && ("postgres" :: "solr" :: "delta" :: "parquet" :: Nil).contains(config.get(S_TABLE_TYPE).get)),
      "Source table shall be in postgres, solr, delta")

  }
}

trait SourceAndTarget extends SourceConf with TargetConf
