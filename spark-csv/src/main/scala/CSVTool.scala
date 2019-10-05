package io.frama.parisni.spark.csv

import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileOutputStream
import java.util.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.jdbc.{ JdbcDialect, JdbcDialects, JdbcType }
import org.apache.spark.sql.types.{ StructType, IntegerType }
import org.apache.spark.sql.SparkSession
import java.io.{ BufferedInputStream, BufferedOutputStream, FileInputStream, FileOutputStream }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.util.UUID.randomUUID
import scala.reflect.io.Directory
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.typesafe.scalalogging.LazyLogging
import io.frama.parisni.spark.dataframe.DFTool
import scala.collection.mutable.ListBuffer

object CSVTool extends LazyLogging {

  def apply(spark: SparkSession, path: String, schema: StructType, delimiter: Option[String] = None, escape: Option[String] = None, multiline: Option[Boolean] = None, dateFormat: Option[String] = None, timestampFormat: Option[String] = None, removeNullRows: Option[String] = None, isCast: Boolean = true): Dataset[Row] = {
    val mandatoryColumns = DFTool.getMandatoryColumns(schema)
    val optionalColumns = DFTool.getOptionalColumns(schema)
    val df = read(spark: SparkSession, path: String, delimiter, escape, multiline, dateFormat, timestampFormat)

    DFTool.existColumns(df, mandatoryColumns)
    val dfWithCol = DFTool.addMissingColumns(df, optionalColumns)
    val dfReorder = DFTool.reorderColumns(dfWithCol, schema)
    val dfNull = if (removeNullRows.isDefined) {
      DFTool.removeNullRows(dfReorder, removeNullRows.get)
    } else {
      dfReorder
    }
    if (isCast)
      DFTool.castColumns(dfNull, schema)
    else
      dfNull
  }

  def write(df: DataFrame, path: String, mode: org.apache.spark.sql.SaveMode) = {
    df.write.format("csv")
      .option("delimiter", ",")
      .option("header", false)
      .option("nullValue", null)
      .option("emptyValue", "\"\"")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("ignoreLeadingWhiteSpace", false)
      .option("ignoreTrailingWhiteSpace", false)
      .mode(mode)
      .save(path)
  }
  
  def read(spark: SparkSession, path: String, delimiter: Option[String] = None, escape: Option[String], multiline: Option[Boolean] = None, dateFormat: Option[String], timestampFormat: Option[String] = None): Dataset[Row] = {
    val headers = getCsvHeaders(spark, path, delimiter)
    val schemaSimple = getStringStructFromArray(headers)
    logger.info(schemaSimple.prettyJson)
    val csvTmp = spark.read.format("csv")
      .schema(schemaSimple)
      .option("multiline", multiline.getOrElse(false))
      .option("delimiter", delimiter.getOrElse(","))
      .option("header", true)
      .option("quote", "\"")
      .option("escape", escape.getOrElse("\""))
      .option("timestampFormat", timestampFormat.getOrElse("yyyy-MM-dd HH:mm:ss"))
      .option("dateFormat", dateFormat.getOrElse("yyyy-MM-dd"))
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .option("mode", "PERMISSIVE")
      .load(path)

    // in case of corrupt record. DIE and show the killer rows
    if (csvTmp.columns.contains("_corrupt_record")) {
      val badRows = csvTmp.filter("`_corrupt_record` is not null").cache
      val numBadRows = badRows.count()
      if (numBadRows > 0) {
        badRows.select("`_corrupt_record`").show(false)
        throw new Exception(numBadRows + " rows are wrong...");
      }
      badRows.unpersist()
    }
    csvTmp

  }

  def getCsvHeaders(spark: SparkSession, path: String, delimiter: Option[String]): Array[String] = {
    val data = spark.sparkContext.textFile(path)
    val header = data.first()
    val headers = header.split(delimiter.getOrElse(","))
    headers

  }

  def getStringStructFromArray(ar: Array[String]): StructType = {
    var struct = new ListBuffer[StructField]()
    for (f <- ar) {
      struct += StructField(f, StringType, true)
    }
    StructType(struct)

  }

}
