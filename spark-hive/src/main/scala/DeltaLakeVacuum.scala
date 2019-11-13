package io.frama.parisni.spark.hive

import io.delta.tables._
import io.frama.parisni.spark.hive.DeltaVacuumYaml._
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.SparkSession

import scala.io.Source

/*
 * Principle
 *
 */
object DeltaLakeVacuum extends App {

  val filename = args(0)
  val LOG = args(1)
  //	  val filename = "test.yaml"

  val ymlTxt = Source.fromFile(filename).mkString
  val yaml = ymlTxt.stripMargin.parseYaml
  val database = yaml.convertTo[Database]

  //
  // SPARK PART
  //
  val spark = SparkSession.builder()
    .appName(database.jobName)
    .getOrCreate()

  spark.sparkContext.setLogLevel(LOG)

  try {
    for (table <- database.tables.getOrElse(Nil)) {
      if (table.isActive.getOrElse(true)) {

        val deltaPath = "%s/%s".format(database.pathDelta, table.tableDelta)
        val deltaTable = DeltaTable.forPath(spark, deltaPath)
        deltaTable.vacuum(table.nbHours.getOrElse(168.0))

      }
    }
  } finally {
    spark.close()
  }

}

