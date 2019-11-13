package io.frama.parisni.spark.hive

import net.jcazevedo.moultingyaml._

object PostgresToHiveYaml extends DefaultYamlProtocol {
  
  case class Database(jobName: String
      , hostPg: String
      , portPg: Int
      , databasePg: String
      , userPg: String
      , tables: Option[List[Table]])
      
  case class Table(tablePg: String
      , tableHive: String
      , schemaPg: String
      , schemaHive: String
      , key: String
      , format: Option[String]
      , delta: Option[String]
      , isActive: Option[Boolean]
      , isMultiline: Option[Boolean]
      , splitFactor: Option[Int]
    	, isAnalyse: Option[Boolean]
      , numThread: Option[Int]){
    
   // require(isMultiline.isEmpty || (isMultiline.isDefined && splitFactor.isDefined), "When multiline then splitfactor must be specified too")
   //require(!isMultiline.get && splitFactor.isDefined && splitFactor.get > 1, "When no multiline, splitfactor should be equal to one")
    require(numThread.isDefined && (numThread.get < 9 && numThread.get > 0), "Thread number should be between 1 and 8")
  }

  implicit val colorFormat = yamlFormat12(Table)
  implicit val paletteFormat = yamlFormat6(Database)

}

