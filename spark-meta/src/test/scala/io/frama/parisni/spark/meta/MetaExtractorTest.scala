package io.frama.parisni.spark.meta

import io.frama.parisni.spark.meta.strategy.MetaStrategyBuilder
import org.apache.spark.sql.{DataFrame, QueryTest}

class MetaExtractorTest extends QueryTest
  with SparkSessionTestWrapper {
/*

  //Database Connection information to adapt with your own test database
  val dbInfo = Map("host" -> "localhost"
    , "user" -> "postgres"
    , "database" -> "postgres"
    , "password" -> "postgres"
    , "port" -> "5432"
    , "schema" -> "public"
    , "dbType" -> "postgresql")

  //To remove or adapt to local configuration
  System.setProperty("hadoop.home.dir", "C:\\winutil")

  val metaExtractor: MetaExtractor = new MetaExtractor(MetaStrategyBuilder.build(), spark
    , dbInfo("host"), dbInfo("database"), dbInfo("user"), dbInfo("dbType"), dbInfo("schema"))


  test("test_addLastCommitTimestampColumn") {

    val meta_df = spark.read.format("io.frama.parisni.spark.postgres")
      .option("query", GetTables.SQL_PG_TABLE.format(dbInfo("database")))
      .option("host", dbInfo("host"))
      .option("user", dbInfo("user"))
      .option("database", dbInfo("database"))
      .option("schema", dbInfo("schema"))
      .option("partitions", 1)
      .option("multiline", value = true)
      .load

    val colName: String = "last_commit_timestampz"
    val dfWithNewCol: DataFrame = metaExtractor.addLastCommitTimestampColumn(meta_df, colName)
    assert(dfWithNewCol.columns.contains(colName))

  }
// */

}
