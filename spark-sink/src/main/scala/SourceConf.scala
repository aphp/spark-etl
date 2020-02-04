trait SourceConf{

  //val config: Map[String, String]

  val S_TABLE_NAME: String = "S_TABLE_NAME"
  val S_TABLE_TYPE: String = "S_TABLE_TYPE"   //"postgres", "solr", "delta", ....
  val S_DATE_FIELD: String = "S_DATE_FIELD"
  //val S_PK: String = "S_PK"                 //list of PKs as: pk1,pk2,pk3,....,pkn

  //def readSource(spark: SparkSession, strings: String*): Any
  def getSourceTableName: Option[String]
  def getSourceTableType: Option[String]
  def getSourceDateField: Option[String]
  //def getSourcePK: Option[String]
}

trait SourceAndTarget extends SourceConf with TargetConf