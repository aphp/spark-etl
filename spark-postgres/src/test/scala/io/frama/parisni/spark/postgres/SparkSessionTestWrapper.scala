package io.frama.parisni.spark.postgres

import com.opentable.db.postgres.junit.{
  EmbeddedPostgresRules,
  SingleInstancePostgresRule
}
import org.apache.spark.sql.SparkSession
import org.junit.Rule

import scala.annotation.meta.getter

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
   val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "4")
     .config("spark.ui.enabled", "false")
     .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark

  }
  // looks like crazy but compatibility issue with junit rule (public)
  @(Rule @getter)
  var pg: SingleInstancePostgresRule = EmbeddedPostgresRules.singleInstance()

  def getPgTool(bulkLoadMode: BulkLoadMode = CSV) =
    PGTool(spark, getPgUrl, "/tmp", bulkLoadMode)

  def getPgUrl =
    pg.getEmbeddedPostgres
      .getJdbcUrl("postgres", "postgres") + "&currentSchema=public"

}
