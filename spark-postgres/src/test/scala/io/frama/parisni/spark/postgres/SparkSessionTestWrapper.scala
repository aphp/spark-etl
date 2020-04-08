package io.frama.parisni.spark.postgres

import com.opentable.db.postgres.junit.{EmbeddedPostgresRules, SingleInstancePostgresRule}
import org.apache.spark.sql.SparkSession
import org.junit.Rule

import scala.annotation.meta.getter

trait SparkSessionTestWrapper {

  // looks like crazy but compatibility issue with junit rule (public)
  @(Rule@getter)
  var pg: SingleInstancePostgresRule = EmbeddedPostgresRules.singleInstance()

  def getPgUrl = pg.getEmbeddedPostgres.getJdbcUrl("postgres", "postgres") + "&currentSchema=public"

  def getPgTool(bulkLoadMode: BulkLoadMode = CSV) = PGTool(spark, getPgUrl, "/tmp", bulkLoadMode)

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

}
