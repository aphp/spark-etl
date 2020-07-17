package io.frama.parisni.spark.hive

import com.typesafe.scalalogging.LazyLogging
import io.frama.parisni.spark.dataframe.DFTool
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * This application copies and overwrites all
  * the tables from one database into the other
  */
object CopyTablesToOtherDatabase extends App with LazyLogging {

  val (fromDb: String,
       toDb: String,
       format: String,
       tableList: Option[String]) = args match {
    case Array(_, _)       => (args(0).toString, args(1).toString, "parquet", None)
    case Array(_, _, _)    => (args(0), args(1), args(2), None)
    case Array(_, _, _, _) => (args(0), args(1), args(2), Some(args(3)))
    case _ =>
      throw new UnsupportedOperationException(
        "shall specify fromDb, toDb and optionally the format")
  }

  implicit val spark = SparkSession
    .builder()
    .appName("")
    .enableHiveSupport()
    .getOrCreate()

  copyAllTables(fromDb, toDb, format, tableList)

  def copyAllTables(
      fromDb: String,
      toDb: String,
      format: String,
      listTable: Option[String] = None)(implicit spark: SparkSession) = {

    val tbList = tableList match {
      case Some(e) => e.split(",")
      case _       => listTables(fromDb)
    }
    for {
      table <- tbList
    }(
      copyTable(fromDb, toDb, table, format)(spark)
    )
  }

  def listTables(fromDb: String)(implicit ss: SparkSession) = {
    val tables: Array[String] =
      ss.catalog
        .listTables(fromDb)
        .filter("tableType = 'MANAGED'") // remove tabel
        .filter("name not rlike 'tmp|[0-7]'") // keep table _d8
        .collect()
        .map(table => table.name)
    logger.info(s"Listed ${tables.size} tables to copy")
    tables
  }

  def copyTable(fromDb: String,
                toDb: String,
                table: String,
                format: String = "parquet")(implicit ss: SparkSession) = {
    logger.info(s"Copying ${fromDb}.${table} TO ${toDb}.${table}")
    val copyTable = ss
      .table(s"${fromDb}.${table}")
      .repartition(200) // compaction of tables
    DFTool.saveHive(copyTable, s"${toDb}.${table}", format)
  }

}
