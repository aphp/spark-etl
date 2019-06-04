package io.frama.parisni.spark.postgres

import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileOutputStream
import java.sql._
import java.util.Properties
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._
import org.postgresql.copy.{CopyManager,PGCopyInputStream}
import org.postgresql.core.BaseConnection;
import scala.util.control.Breaks._
import org.apache.spark.sql.SparkSession
import java.io.{BufferedInputStream, BufferedOutputStream, FileInputStream, FileOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.util.UUID.randomUUID
import scala.reflect.io.Directory
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD


object DFtools extends java.io.Serializable {

  def dfAddHash( df:Dataset[Row], columnsToExclude:List[String] = Nil ): Dataset[Row] = {

    df.withColumn("hash", hash(df.columns.filter(x => !columnsToExclude.contains(x)).map( x => col(x) ): _*))

  }
}
