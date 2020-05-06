package io.frama.parisni.spark.sync.copy

import net.jcazevedo.moultingyaml.DefaultYamlProtocol

object DeltaToDeltaYaml extends DefaultYamlProtocol {

  case class Database(jobName: String
      , timestampTable:Option[String]
      , timestampLastColumn:Option[String]      // To be used as PG  "S_DATE_FIELD"
      , timestampColumns:Option[List[String]]   // To be used as Delta dates
      , dateMax: Option[String]                 // Added param
      , tables: Option[List[Table]])

  case class Table(tableDeltaSource: String
      , tableDeltaTarget: String
      , path: String
      //, pathTarget: String
      , key: List[String]
      , typeLoad: Option[String]      // Added param
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
