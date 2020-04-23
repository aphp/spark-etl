/**
 * Commenting this class as the performance gain is only visible when
 * writing at least some amount of data, meaning tests are long.
 */
/*
package io.frama.parisni.spark.postgres

import java.sql.Timestamp

import org.apache.spark.sql.types._
import org.apache.spark.sql.{QueryTest, Row}
import org.junit.Test


class TestPGBulkLoadPerf extends QueryTest with SparkSessionTestWrapper {

  val rowNumber = 1000000

  val schema = StructType(Seq(
    StructField("f1", LongType, nullable=true),
    StructField("f2", StringType, nullable=true),
    StructField("f3", DoubleType, nullable=true),
    StructField("f4", StringType, nullable=true),
    StructField("f5", StringType, nullable=true),
    StructField("f6", StringType, nullable=true),
    StructField("f7", StringType, nullable=true),
    StructField("f8", StringType, nullable=true),
    StructField("f9", StringType, nullable=true),
    StructField("f10", TimestampType, nullable=true)
  ))

  val row = Row.fromTuple((
    12L,
    "first string value",
    3.14159d,
    "second string value",
    "q'odkjdfvlkmsdvmlkxscl metgh]lkmndm089223ri8pio12mv,qefl,,l;w;flwetg,,,wegkl;w,wergmk",
    "q'odkjdfvlkmsdvmlkxscl metgh]lkmndm089223ri8pio12mv,qefl,,l;w;flwetg,,,wegkl;w,wergmk",
    null,
    "lwmjolfdWRgjokj234]09tip9iueomvmKLWemf/\\/\\wekf,kwemk'm,qkaAETAKLKwiljlaj",
    "lwmjolfdWRgjokj234]09tip9iueomvmKLWemf/\\/\\wekf,kwemk'm,qkaAETAKLKwiljlaj",
    new Timestamp(System.currentTimeMillis())
    ))

  val rows = (1 to rowNumber).map(i => Row.fromSeq(i.toLong +: row.toSeq.tail))

  val df = spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), schema)

  def abstractTest(loadType: String, bulkLoadBufferSize: String): Unit = {
    // Force materialization of dataframe before test
    df.count()
    val tableName = s"test_perf_${loadType}_${bulkLoadBufferSize}_table"
    val t0 = System.nanoTime()
    df.write.format("io.frama.parisni.spark.postgres")
      .option("host", "localhost")
      .option("port", pg.getEmbeddedPostgres.getPort)
      .option("database", "postgres")
      .option("user", "postgres")
      .option("table", tableName)
      .option("partitions", "4")
      .option("bulkLoadMode", s"$loadType")
      .option("bulkLoadBufferSize", bulkLoadBufferSize)
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save
    val duration = System.nanoTime() - t0

    val db = pg.getEmbeddedPostgres.getPostgresDatabase
    val conn = db.getConnection()
    conn.createStatement().execute(s"""DROP TABLE "$tableName" """)

    println(s"$loadType($bulkLoadBufferSize): $duration")
  }

  @Test def writeRowsCSVUsualBuffer(): Unit = {
    abstractTest("csv", "65536")
  }

  @Test def writeRowsPgBulkInsertUsualBuffer(): Unit = {
    abstractTest("PgBulkInsert", "65536")
  }

  @Test def writeRowsPgBulkInsertBigBuffer(): Unit = {
    abstractTest("PgBulkInsert", "1048576")
  }
}
*/