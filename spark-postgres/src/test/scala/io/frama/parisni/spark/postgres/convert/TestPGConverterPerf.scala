/**
 * Commenting this class as the performance gain is only visible when
 * writing at least some amount of data, meaning tests are long.
 */
/*
package io.frama.parisni.spark.postgres.convert

import java.io.StringWriter
import java.sql.Timestamp
import java.util.TimeZone

import io.frama.parisni.spark.postgres.SparkSessionTestWrapper
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{QueryTest, Row}
import org.junit.Test


class TestPGConverterPerf extends QueryTest with SparkSessionTestWrapper {

  val rowNumber = 100000

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
    "q'odkjdfvlkmsdvmlkxscl metgh]lkmndm089223ri8pio12mv,qefl,,l;w;flwetg,,,wegkl;w,wergmk",
    "lwmjolfdWRgjokj234]09tip9iueomvmKLWemf/\\/\\wekf,kwemk'm,qkaAETAKLKwiljlaj",
    "lwmjolfdWRgjokj234]09tip9iueomvmKLWemf/\\/\\wekf,kwemk'm,qkaAETAKLKwiljlaj",
    new Timestamp(System.currentTimeMillis())
    ))

  val rows = (1 to rowNumber).map(i => Row.fromSeq(i.toLong +: row.toSeq.tail))

  @Test def convertRowsManual(): Unit = {
    val converters = schema.map(t => PGConverter.makeConverter(t.dataType)).toArray
    val t0 = System.nanoTime()
    rows.map(r => PGConverter.convertRow(r, schema.length, ",", converters))
    val duration = System.nanoTime() - t0
    println(duration)
  }

  @Test def writeRowsCSV(): Unit = {

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), schema)
    val dest = "/tmp/aphp/spark-etl/spark-postgres/test.csv"

    val t0 = System.nanoTime()
    df.write.format("csv")
      .option("delimiter", ",")
      .option("header", "false")
      .option("nullValue", null)
      .option("emptyValue", "\"\"")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("ignoreLeadingWhiteSpace", "false")
      .option("ignoreTrailingWhiteSpace", "false")
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .save(dest)
    val duration = System.nanoTime() - t0
    println(duration)

  }

  @Test def convert100KInternalRowsUnivocity(): Unit = {

    val csvOptionsMap = Map(
      "delimiter" -> ",",
      "header" -> "false",
      "nullValue" -> null,
      "emptyValue" -> "\"\"",
      "quote" -> "\"",
      "escape" -> "\"",
      "ignoreLeadingWhiteSpace" -> "false",
      "ignoreTrailingWhiteSpace" -> "false"
    )
    val csvOptions = new CSVOptions(csvOptionsMap, columnPruning=true, TimeZone.getDefault.getID)

    val writer = new StringWriter()

    val univocityGenerator = new UnivocityGenerator(schema, writer, csvOptions)
    val rowEncoder = RowEncoder.apply(schema)

    val t0 = System.nanoTime()
    rows.foreach(r => univocityGenerator.write(rowEncoder.toRow(r)))
    val duration = System.nanoTime() - t0
    println(duration)

    assert(writer.toString.startsWith("1,"))
    writer.close()

  }
}
*/