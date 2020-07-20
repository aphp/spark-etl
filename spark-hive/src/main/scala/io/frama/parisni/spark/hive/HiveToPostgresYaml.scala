package io.frama.parisni.spark.hive

import net.jcazevedo.moultingyaml._

object HiveToPostgresYaml extends DefaultYamlProtocol {

  case class Table(
      tableHive: String,
      tablePg: String,
      key: List[String],
      hash: Option[String],
      schemaHive: String,
      numThread: Option[Int],
      insertDatetime: Option[String],
      pk: Option[String],
      updateDatetime: Option[String],
      deleteDatetime: Option[String],
      typeLoad: Option[String],
      joinTable: Option[String],
      filter: Option[String],
      bulkLoadMode: Option[String],
      deleteSet: Option[String],
      joinPostgresColumn: Option[String],
      joinHiveColumn: Option[String],
      joinKeepColumn: Option[Boolean],
      joinFetchColumns: Option[List[String]],
      isActive: Option[Boolean],
      reindex: Option[Boolean],
      format: Option[String]
  ) {
    require(
      numThread.isDefined && (numThread.get < 20 && numThread.get > 0),
      "Thread number should be between 1 and 19"
    )
    require(
      typeLoad.isEmpty || (Array("full", "megafull", "scd1", "scd2")
        .contains(typeLoad.get)),
      "When update a date field should be specified"
    )
    require(
      joinTable.isEmpty && joinPostgresColumn.isEmpty && joinFetchColumns.isEmpty && joinHiveColumn.isEmpty && joinKeepColumn.isEmpty ||
        joinTable.isDefined && joinPostgresColumn.isDefined && joinFetchColumns.isDefined && joinHiveColumn.isDefined && joinKeepColumn.isDefined,
      "Either join* should be empty OR defined"
    )
  }

  case class Database(
      jobName: String,
      hostPg: String,
      portPg: Int,
      userPg: String,
      databasePg: String,
      schemaPg: String,
      tables: Option[List[Table]] = None
  )

  implicit val colorFormat = yamlFormat22(Table)
  implicit val paletteFormat = yamlFormat7(Database)
}
