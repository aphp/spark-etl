package io.frama.parisni.spark.sync.copy

import net.jcazevedo.moultingyaml.DefaultYamlProtocol

object ParquetToSolrYaml extends DefaultYamlProtocol {

  case class Database(jobName: String
      , timestampTable:Option[String]
      , timestampLastColumn:Option[String]      // To be used as Source "S_DATE_FIELD"
      , timestampColumns:Option[List[String]]   // To be used as Target dates
      , dateMax: Option[String]                 // Added param
      , tables: Option[List[Table]])

  case class Table(tableParquet: String
                   , tableSolr: String
                   , schemaParquet: String
                   , ZkHost: String
                   , key: List[String]
                   , typeLoad: Option[String] // Added param
                   , format: Option[String]
                   , delta: Option[String]
                   , isActive: Option[Boolean]
                   , isMultiline: Option[Boolean]
                   , splitFactor: Option[Int]
                   , isAnalyse: Option[Boolean]
                   , numThread: Option[Int]){

    require(numThread.isDefined && (numThread.get < 9 && numThread.get > 0), "Thread number should be between 1 and 8")
  }

  implicit val colorFormat = yamlFormat13(Table)
  implicit val paletteFormat = yamlFormat6(Database)

}
