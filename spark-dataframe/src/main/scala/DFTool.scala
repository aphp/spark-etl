package io.frama.parisni.spark.dataframe

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


object DFTool extends LazyLogging {

def reorderColumns(df: Dataset[Row], schema: StructType): Dataset[Row] = {
    val reorderedColumnNames = schema.fieldNames
    df.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)
  }

  def castColumns(df: Dataset[Row], schema: StructType): Dataset[Row] = {
    val newDf = df.sparkSession.createDataFrame(df.rdd, schema)
    val trDf = newDf.schema.fields.foldLeft(df) {
      (df, s) => df.withColumn(s.name, df(s.name).cast(s.dataType))
    }
    df.sparkSession.createDataFrame(trDf.rdd, schema)
  }

  def existColumns(df: Dataset[Row], columnsNeeded: StructType) = {
    var tmp = ""
    val columns = df.columns
    for (column <- columnsNeeded.fieldNames) {
      if (!columns.contains(column))
        tmp += column + ", "
    }
    if (tmp != "") {
      throw new Exception(f"Missing columns in the csv: [${tmp}]")
    }
  }

  def getMandatoryColumns(schema: StructType): StructType = {
    StructType(schema.filter(f => !f.metadata.contains("default")))
  }

  def getOptionalColumns(schema: StructType): StructType = {
    StructType(schema.filter(f => f.metadata.contains("default")))
  }

  def addMissingColumns(df: Dataset[Row], missingSchema: StructType): Dataset[Row] = {
    var result = df
    missingSchema.fields.foreach(
      f => {
        logger.debug(f"Added ${f.name} column")
        result = result.withColumn(f.name, lit(f.metadata.getString("default")).cast(f.dataType))

      })
    result
  }
  
}
