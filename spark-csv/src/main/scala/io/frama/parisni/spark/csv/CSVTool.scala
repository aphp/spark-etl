package io.frama.parisni.spark.csv

import java.io.{File, FileOutputStream}
import java.nio.file.{Files, InvalidPathException, Paths}

import com.typesafe.scalalogging.LazyLogging
import io.frama.parisni.spark.dataframe.DFTool
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructType, _}

import scala.collection.mutable.ListBuffer

// important the case class outside the scope
case class exportDf(file: String, content: String)

object CSVTool extends LazyLogging {

  def apply(
      spark: SparkSession,
      path: String,
      schema: StructType,
      delimiter: Option[String] = None,
      escape: Option[String] = None,
      multiline: Option[Boolean] = None,
      dateFormat: Option[String] = None,
      timestampFormat: Option[String] = None,
      removeNullRows: Option[String] = None,
      isCast: Boolean = true
  ): Dataset[Row] = {
    val mandatoryColumns = DFTool.getMandatoryColumns(schema)
    val optionalColumns = DFTool.getOptionalColumns(schema)
    val df = read(
      spark: SparkSession,
      path: String,
      delimiter,
      escape,
      multiline,
      dateFormat,
      timestampFormat
    )

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

  def write(
      df: DataFrame,
      path: String,
      mode: org.apache.spark.sql.SaveMode
  ): Unit = {
    df.write
      .format("csv")
      .option("delimiter", ",")
      .option("header", value = false)
      .option("nullValue", null)
      .option("emptyValue", "\"\"")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("ignoreLeadingWhiteSpace", value = false)
      .option("ignoreTrailingWhiteSpace", value = false)
      .mode(mode)
      .save(path)
  }

  def read(
      spark: SparkSession,
      path: String,
      delimiter: Option[String] = None,
      escape: Option[String],
      multiline: Option[Boolean] = None,
      dateFormat: Option[String],
      timestampFormat: Option[String] = None
  ): Dataset[Row] = {
    val headers = getCsvHeaders(spark, path, delimiter)
    val schemaSimple = getStringStructFromArray(headers)
    logger.info(schemaSimple.prettyJson)
    val csvTmp = spark.read
      .format("csv")
      .schema(schemaSimple)
      .option("multiline", multiline.getOrElse(false))
      .option("delimiter", delimiter.getOrElse(","))
      .option("header", value = true)
      .option("quote", "\"")
      .option("escape", escape.getOrElse("\""))
      .option(
        "timestampFormat",
        timestampFormat.getOrElse("yyyy-MM-dd HH:mm:ss")
      )
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
        throw new Exception(numBadRows + " rows are wrong...")
      }
      badRows.unpersist()
    }
    csvTmp

  }

  def getCsvHeaders(
      spark: SparkSession,
      path: String,
      delimiter: Option[String]
  ): Array[String] = {
    val data = spark.sparkContext.textFile(path)
    val header = data.first()
    val headers = header.split(delimiter.getOrElse(","))
    headers

  }

  def getStringStructFromArray(ar: Array[String]): StructType = {
    var struct = new ListBuffer[StructField]()
    for (f <- ar) {
      struct += StructField(f, StringType, nullable = true)
    }
    StructType(struct)

  }

  /**
    * Exports local files
    *
    * @param df            a dataframe with
    * @param fileColumn    shall be a string
    * @param contentColumn shall be a string
    * @param folder
    */
  def writeDfToLocalFiles(
      df: DataFrame,
      fileColumn: String,
      contentColumn: String,
      folder: String
  ) = {

    // validate folder exists
    if (!Files.exists(Paths.get(folder)))
      throw new InvalidPathException(folder, folder)

    import df.sparkSession.implicits._
    val ds = df
      .select(
        col(fileColumn).cast(StringType) as "file",
        col(contentColumn).cast(StringType) as "content"
      )
      .as[exportDf]

    ds.collect()
      .foreach(p => {
        val fileName = folder + "/" + p.file + ".txt"
        val writerAnn = new java.io.PrintWriter(fileName, "UTF-8")
        if (p.content != null)
          writerAnn.write(p.content)
        writerAnn.close()
      })
    logger.info(s"Exported ${ds.count} files")

  }

  def writeCsvLocal(
      df: DataFrame,
      tempPath: String,
      localPath: String,
      options: Map[String, String] = Map(),
      format: String = "csv"
  ) = {
    val hdfs = FileSystem.get(new Configuration())
    val hdfsPath = new Path(tempPath)
    val targetFile = new File(localPath)
    val fileWDot = new File(
      targetFile.getPath.substring(
        0,
        targetFile.getPath.length - targetFile.getName.length
      ) + "." + targetFile.getName
    )
    logger.warn(s"writing to temp file ${fileWDot.getAbsolutePath}")
    val mime = format match {
      case "csv"  => ".csv"
      case "text" => ".txt"
      case _      => throw new Exception("only text and csv")
    }
    try {
      df.write
        .mode(SaveMode.Overwrite)
        .format(format)
        .options(options)
        .save(tempPath)
      val it = hdfs.listFiles(hdfsPath, false)
      val colFile = new FileOutputStream(fileWDot, false)
      colFile.write((df.columns.mkString(",") + "\n").getBytes)
      colFile.close()
      while (it.hasNext) {
        val file = it.next().getPath.toString
        if (file.endsWith(mime)) {
          val stream = hdfs.open(new Path(file.toString)).getWrappedStream
          val outStream = new FileOutputStream(fileWDot, true)

          val bytes = new Array[Byte](10024) //1024 bytes - Buffer size
          Iterator
            .continually(stream.read(bytes))
            .takeWhile(-1 !=)
            .foreach(read => outStream.write(bytes, 0, read))
          outStream.close()
        }
      }
    } finally {
      hdfs.delete(hdfsPath, true)
      logger.warn(s"deleting hdfs path $hdfsPath")
      fileWDot.renameTo(targetFile)
      logger.warn(
        s"renaming ${fileWDot.getAbsolutePath} to ${targetFile.getAbsolutePath}"
      )
    }
  }

}
