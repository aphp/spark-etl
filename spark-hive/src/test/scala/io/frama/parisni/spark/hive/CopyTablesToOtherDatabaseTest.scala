package io.frama.parisni.spark.hive

import org.apache.spark.sql.QueryTest

case class Table1(id: Long, cd: String)
case class Table2(id: Long, lib: String)

class CopyTablesToOtherDatabaseTest extends QueryTest with SparkTestingUtil {

  test("verify tables are overwriten") {
    spark.sql("create database if not exists sourcedb")
    spark.sql("create database if not exists targetdb")

    import spark.implicits._
    val table1 =
      List(Table1(1L, "bob"), Table1(2L, "jim"), Table1(3L, "john")).toDS
    table1.write
      .saveAsTable("sourcedb.table1")

    val table2 = List(Table2(1L, "jim"))
    table2.toDS.write.saveAsTable("sourcedb.table2")

    CopyTablesToOtherDatabase.copyAllTables("sourcedb", "targetdb", "parquet")

    checkAnswer(spark.table("targetdb.table1"), table1.toDF)
    checkAnswer(spark.table("targetdb.table2"), table2.toDF)
  }

}
