package io.frama.parisni.spark.hive

import com.typesafe.scalalogging.LazyLogging
import io.frama.parisni.spark.dataframe.DFTool
import io.frama.parisni.spark.hive.HiveToPostgresYaml._
import io.frama.parisni.spark.postgres.PGTool
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.io.Source

/*
 * Principle
 *
 *
 */
object HiveToPostgres extends App with LazyLogging {

  val filename = args(0)
  //	  val filename = "test.yaml"

  val ymlTxt = Source.fromFile(filename).mkString
  val yaml = ymlTxt.stripMargin.parseYaml
  val database = yaml.convertTo[Database]

  val spark = SparkSession.builder()
    .appName(database.jobName)
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val url = f"jdbc:postgresql://${database.hostPg}:${database.portPg}/${database.databasePg}?user=${database.userPg}&currentSchema=${database.schemaPg}"
  val pg = PGTool(spark, url, "spark-postgres")
  try {
    for (table <- database.tables.getOrElse(Nil)) {
      if (table.isActive.getOrElse(true)) {
        logger.warn(f"LOADING $table.tableHive")
        val query = f"select * from ${table.schemaHive}.${table.tableHive}"
        var dfHive = spark.sql(query)

        //IN CASE JOIN is defined
        if (table.joinTable.isDefined) {
          // get the information from postgres
          val joinTable = pg.inputBulk("select %s, %s from %s".format(table.joinPostgresColumn.get, table.joinFetchColumns.get.mkString(", "), table.joinTable.get),
            isMultiline = Some(false), numPartitions = Some(1))
          // join to extend the hive table
          dfHive = dfHive
            .alias("h")
            .join(joinTable.as("t"), col("h.%s".format(table.joinHiveColumn.get)) === col("t.%s".format(table.joinPostgresColumn.get)), "left")
            .drop("t.%s".format(table.joinPostgresColumn.get))

          if (!table.joinKeepColumn.get)
            dfHive = dfHive.drop(table.joinHiveColumn.get)
        }

        var df = DFTool.dfAddHash(dfHive)
        table.typeLoad.getOrElse("scd1") match {
          case "scd1" => pg.outputScd1Hash(table = table.tablePg, key = table.key, df = df, numPartitions = table.numThread)
          case "full" => {
            pg.tableTruncate(table.tablePg)
            pg.outputBulk(table.tablePg, df, 8, reindex = true)
          }
          case _ => throw new UnsupportedOperationException
        }
        logger.warn(f"LOADED $table.tableHive")
      }
    }
  } finally {
    pg.purgeTmp()
  }
}

