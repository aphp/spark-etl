package io.frama.parisni.spark.sync.copy

import net.jcazevedo.moultingyaml.DefaultYamlProtocol

object PostgresToDeltaYaml extends DefaultYamlProtocol {

  case class Database(
      jobName: String,
      hostPg: String,
      portPg: Int,
      databasePg: String,
      userPg: String,
      timestampTable: Option[String],
      databaseHive: Option[String],
      timestampLastColumn: Option[String] // To be used as PG  "S_DATE_FIELD"
      ,
      timestampColumns: Option[List[String]] // To be used as Delta dates
      ,
      dateMax: Option[String] // Added param
      ,
      tables: Option[List[Table]]
  )

  case class Table(
      tablePg: String,
      tableHive: String,
      schemaPg: String,
      schemaHive: String,
      key: List[String] // Changed from String -> Option[List[String]]
      ,
      typeLoad: Option[String] // Added param
      ,
      format: Option[String],
      delta: Option[String],
      isActive: Option[Boolean],
      isMultiline: Option[Boolean],
      splitFactor: Option[Int],
      isAnalyse: Option[Boolean],
      numThread: Option[Int]
  ) {

    require(
      numThread.isDefined && (numThread.get < 9 && numThread.get > 0),
      "Thread number should be between 1 and 8"
    )
  }

  implicit val colorFormat = yamlFormat13(Table)
  implicit val paletteFormat = yamlFormat11(Database)

}
