package io.frama.parisni.spark.csv

import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileOutputStream
import java.util.Properties
import org.apache.spark.sql.Dataset
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
import org.apache.spark.sql.types.{ DataTypes, StructType }
import com.typesafe.scalalogging.LazyLogging
import io.frama.parisni.spark.dataframe.DFTool

object CSVTool extends LazyLogging {

  def apply(spark: SparkSession, path: String, schema: StructType, delimiter: Option[String] = None, escape: Option[String] = None, multiline: Option[Boolean] = None, dateFormat: Option[String] = None, timestampFormat: Option[String] = None): Dataset[Row] = {
    val mandatoryColumns = DFTool.getMandatoryColumns(schema)
    val optionalColumns = DFTool.getOptionalColumns(schema)
    val df = read(spark: SparkSession, path: String, delimiter, escape, multiline, dateFormat, timestampFormat)

    DFTool.existColumns(df, mandatoryColumns)
    val dfWithCol = DFTool.addMissingColumns(df, optionalColumns)
    val dfReorder = DFTool.reorderColumns(dfWithCol, schema)
    val result = DFTool.castColumns(dfReorder, schema)
    result
  }

  def read(spark: SparkSession, path: String, delimiter: Option[String] = None, escape: Option[String], multiline: Option[Boolean] = None, dateFormat: Option[String], timestampFormat: Option[String] = None): Dataset[Row] = {
    spark.read.format("csv")
      .option("multiline", multiline.getOrElse(false))
      .option("delimiter", delimiter.getOrElse(","))
      .option("header", true)
      .option("quote", "\"")
      .option("escape", escape.getOrElse("\""))
      .option("timestampFormat", timestampFormat.getOrElse("yyyy-MM-dd HH:mm:ss"))
      .option("dateFormat", dateFormat.getOrElse("yyyy-MM-dd"))
      .option("mode", "FAILFAST")
      .load(path)
  }

}
