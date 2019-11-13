package io.frama.parisni.spark.hive

import io.delta.tables._
import io.frama.parisni.spark.dataframe.DFTool
import io.frama.parisni.spark.hive.PostgresToHiveYaml._
import io.frama.parisni.spark.postgres.PGTool
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.SparkSession

import scala.io.Source
/*
 * Principle
 *
 *
 */
object PostgresToHive extends App {

  val filename = args(0)
  //	  val filename = "test.yaml"

  val ymlTxt = Source.fromFile(filename).mkString
  val yaml = ymlTxt.stripMargin.parseYaml
  val database = yaml.convertTo[Database]

  //
  // SPARK PART
  //
  val spark = SparkSession.builder()
    .appName(database.jobName)
    .enableHiveSupport()
    .getOrCreate()

  val url = f"jdbc:postgresql://${database.hostPg}:${database.portPg}/${database.databasePg}?user=${database.userPg}"
  val pgClient = PGTool(spark, url, "spark-postgres")

  try {
    for (table <- database.tables.getOrElse(Nil)) {
      if (table.isActive.getOrElse(true)) {
        val query = f"select * from ${table.schemaPg}.${table.tablePg} " + table.delta.getOrElse("")

        
        val pgTable = DFTool.dfAddHash(pgClient.inputBulk(query = query, isMultiline = table.isMultiline, numPartitions = table.numThread, splitFactor = table.splitFactor, partitionColumn = table.key))
        table.format.getOrElse("orc") match {
          case "delta" => {

            val deltaPath = "%s/%s".format(table.schemaHive, table.tableHive)
            if (table.delta.isDefined) {
              val deltaTable = DeltaTable.forPath(spark, deltaPath)
              deltaTable
                .as("t")
                .merge(
                  pgTable.as("s"),
                  "s.%s = t.%s".format(table.key, table.key))
                .whenMatched("s.hash <> t.hash")
                .updateAll()
                .whenNotMatched()
                .insertAll()
                .execute()
            } else {
              // full mode
              pgTable.write.format("delta").mode(Overwrite).save(deltaPath)
            }
          }
          case format => pgTable.write.format(format).mode(Overwrite).saveAsTable(f"${table.schemaHive}.${table.tableHive}")
        }
      }
    }
  } finally {
    pgClient.purgeTmp()
  }
  spark.close()

  //
  // HIVE PART
  //
  /*
  val user = System.getProperty("user.name")
  val hiveClient = HIVETool(f"$user@<REALM>", f"/export/home/$user/$user.keytab", "jdbc:hive2://<host>:<port>/default;principal=hive/_HOST@<REALM>")

  try {
    for (table <- database.tables.getOrElse(Nil)) {
      if (table.isAnalyse.getOrElse(false)) {
        hiveClient.execute(f"analyze table ${table.schemaHive}.${table.tableHive} compute statistics for columns")
        hiveClient.execute(f"analyze table ${table.schemaHive}.${table.tableHive} compute statistics")
      }
    }
  } finally {
    hiveClient.close()
  }

   */

}

