package io.frama.parisni.spark.postgres

import java.io._
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSetMetaData}
import java.util.Properties
import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.postgresql.copy.{CopyManager, PGCopyInputStream}
import org.postgresql.core.BaseConnection

import scala.concurrent.{Await, TimeoutException, Promise}
import scala.concurrent.duration.Duration
import scala.util.Try

sealed trait BulkLoadMode
object CSV extends BulkLoadMode
object Stream extends BulkLoadMode


class PGTool(spark: SparkSession
             , url: String
             , tmpPath: String
             , bulkLoadMode: BulkLoadMode
            ) {

  private var password: String = ""

  def setPassword(pwd: String = ""): PGTool = {
    password = PGTool.passwordFromConn(url, pwd)
    this
  }

  def showPassword(): Unit = {
    println(password)
  }

  private def genPath: String = {
    tmpPath + "/" + randomUUID.toString
  }

  def purgeTmp(): Boolean = {
    val defaultFSConf = spark.sessionState.newHadoopConf().get("fs.defaultFS")
    val fsConf = if (tmpPath.startsWith("file:")) {
      "file:///"
    } else {
      defaultFSConf
    }
    val conf = new Configuration()
    conf.set("fs.defaultFS", fsConf)
    val fs = FileSystem.get(conf)
    fs.deleteOnExit(new Path(tmpPath)) // delete file when spark quits
  }

  /**
   * Copy a table from an other table excluding data
   *
   * @param tableSrc   String
   * @param tableTarg  String
   * @param isUnlogged Boolean
   *
   *
   */
  def tableCopy(tableSrc: String, tableTarg: String, isUnlogged: Boolean = true): PGTool = {
    PGTool.tableCopy(url, tableSrc, tableTarg, password, isUnlogged)
    this
  }

  def tableCreate(tableTarg: String, schema: StructType, isUnlogged: Boolean = true): PGTool = {
    PGTool.tableCreate(url, tableTarg, schema, password, isUnlogged = false)
    this
  }

  def tableMove(tableSrc: String, tableTarg: String): PGTool = {
    PGTool.tableMove(url, tableSrc, tableTarg, password)
    this
  }

  def tableTruncate(table: String): PGTool = {
    PGTool.tableTruncate(url, table, password)
    this
  }

  def tableDrop(table: String): PGTool = {
    PGTool.tableDrop(url, table, password)
    this
  }

  def tableRename(from: String, to: String): PGTool = {
    PGTool.tableRename(url, from, to, password)
    this
  }

  def killLocks(table: String): Int = {
    PGTool.killLocks(url, table, password)
  }

  def sqlExec(query: String): PGTool = {
    PGTool.sqlExec(url, query, password)
    this
  }

  def sqlExecWithResult(query: String): Dataset[Row] = {
    PGTool.sqlExecWithResult(spark, url, query, password)
  }

  /**
   * Get a spark dataframe from a postgres SQL using
   * the built-in COPY
   *
   * @return DataFrame
   *
   */
  def inputBulk(query: String, isMultiline: Option[Boolean] = None, numPartitions: Option[Int] = None, splitFactor: Option[Int] = None, partitionColumn: String = ""): Dataset[Row] = {
    PGTool.inputQueryBulkDf(spark, url, query, genPath, isMultiline.getOrElse(false), numPartitions.getOrElse(1), partitionColumn, splitFactor.getOrElse(1), password = password)
  }

  /**
   * Write a spark dataframe into a postgres table using
   * the built-in COPY
   *
   */
  def outputBulk(table: String, df: Dataset[Row], numPartitions: Int = 8, reindex: Boolean = false): PGTool = {
    PGTool.outputBulk(spark, url, table, df, genPath, numPartitions, password, reindex, bulkLoadMode)
    this
  }

  def input(query: String, numPartitions: Int = 1, partitionColumn: String = ""): Dataset[Row] = {
    PGTool.inputQueryDf(spark, url, query, numPartitions, partitionColumn, password)
  }

  def output(table: String, df: Dataset[Row], batchsize: Int = 50000): PGTool = {
    PGTool.output(url, table, df, batchsize, password)
    this
  }

  def outputScd2Hash(table: String, df: DataFrame, pk: String, key: List[String], endDatetimeCol: String, partitions: Option[Int] = None, multiline: Option[Boolean] = None): Unit = {

    PGTool.outputBulkDfScd2Hash(spark, url, table, df, pk, key, endDatetimeCol, partitions.getOrElse(4), genPath, password)
  }

  def outputScd1Hash(table: String, key: List[String], df: Dataset[Row], numPartitions: Option[Int] = None, filter: Option[String] = None, deleteSet: Option[String] = None): PGTool = {

    PGTool.outputBulkDfScd1Hash(spark, url, table, df, key, numPartitions.getOrElse(4), genPath, filter, deleteSet, password)
    this
  }

  def outputBulkCsv(table: String, columns: String, path: String, numPartitions: Int = 8, delimiter: String = ",", csvPattern: String = ".*.csv"): PGTool = {
    PGTool.outputBulkCsvLow(spark, url, table, columns, path, numPartitions, delimiter, csvPattern, password)
    this
  }

  def getSchemaQuery(query: String): StructType = {
    PGTool.getSchemaQuery(spark, url, query, password)
  }

}

object PGTool extends java.io.Serializable with LazyLogging {

  def apply(spark: SparkSession, url: String, tmpPath: String, bulkLoadMode: BulkLoadMode = CSV): PGTool = {
    new PGTool(spark, url, tmpPath + "/spark-postgres-" + randomUUID.toString, bulkLoadMode).setPassword("")
  }

  private def dbPassword(hostname: String, port: String, database: String, username: String): String = {
    // Usage: val thatPassWord = dbPassword(hostname,port,database,username)
    // .pgpass file format, hostname:port:database:username:password

    val fs = FileSystem.get(new java.net.URI("file:///"), new Configuration)
    val reader = new BufferedReader(new InputStreamReader(fs.open(new Path(scala.sys.env("HOME"), ".pgpass"))))
    val content = Iterator.continually(reader.readLine()).takeWhile(_ != null).mkString("\n")
    var passwd = ""
    content.split("\n").foreach {
      line =>
        val connCfg = line.split(":")
        if (hostname == connCfg(0)
          && port == connCfg(1)
          && (database == connCfg(2) || connCfg(2) == "*")
          && username == connCfg(3)) {
          passwd = connCfg(4)
        }
    }
    reader.close()
    passwd
  }

  def passwordFromConn(url: String, password: String): String = {
    if (!password.isEmpty) {
      return password
    }
    val pattern = "jdbc:postgresql://(.*):(\\d+)/(\\w+)[?]user=(\\w+).*".r
    val pattern(host, port, database, username) = url
    dbPassword(host, port, database, username)
  }

  def connOpen(url: String, password: String = ""): Connection = {
    val prop = new Properties()
    prop.put("password", passwordFromConn(url, password))
    val dbc: Connection = DriverManager.getConnection(url, prop)
    dbc
  }

  private def getSchemaQuery(spark: SparkSession, url: String, query: String, password: String = ""): StructType = {
    val queryStr = s"""(SELECT * FROM ($query) as tmp1 LIMIT 0) as tmp"""
    spark.read.format("jdbc")
      .option("url", url)
      .option("password", passwordFromConn(url, password))
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", queryStr)
      .load.schema
  }

  def tableTruncate(url: String, table: String, password: String = ""): Unit = {
    val conn = connOpen(url, password)
    val st: PreparedStatement = conn.prepareStatement(s"""TRUNCATE TABLE "$table" """)
    st.executeUpdate()
    conn.close()
  }

  def tableDrop(url: String, table: String, password: String = ""): Unit = {
    val conn = connOpen(url, password)
    val st: PreparedStatement = conn.prepareStatement(s"""DROP TABLE IF EXISTS "$table" """)
    st.executeUpdate()
    conn.close()
  }

  def tableRename(url: String, from: String, to: String, password: String = ""): Unit = {
    val conn = connOpen(url, password)
    val st: PreparedStatement = conn.prepareStatement(s"""ALTER TABLE "$from" RENAME TO "$to" """)
    st.executeUpdate()
    conn.close()
  }

  def killLocks(url: String, table: String, password: String): Int = {
    var killed = 0
    val conn = connOpen(url, password)
    try {
      val st = conn.prepareStatement(
        "select pid from pg_locks l join pg_class t on l.relation = t.oid and t.relkind = 'r' where t.relname = ?")
      st.setString(1, table)
      val rs = st.executeQuery()
      val st2 = conn.prepareStatement("select pg_cancel_backend(?) AND pg_terminate_backend(?)")

      while(rs.next()) {
        val pid = rs.getInt("pid")
        st2.setInt(1, pid)
        st2.setInt(2, pid)
        st2.executeQuery()
        killed += 1
      }
    } finally {
      conn.close()
    }
    killed
  }

  def sqlExec(url: String, query: String, password: String = ""): Unit = {
    val conn = connOpen(url, password)
    val st: PreparedStatement = conn.prepareStatement(s"$query")
    st.executeUpdate()
    conn.close()
  }

  def sqlExecWithResult(spark: SparkSession, url: String, query: String, password: String = ""): Dataset[Row] = {
    val conn = connOpen(url, password)
    try {

      val st = conn.createStatement()
      val rs = st.executeQuery(query)

      import scala.collection.mutable.ListBuffer
      var c = new ListBuffer[Row]()
      while (rs.next()) {
        val b = (1 to rs.getMetaData.getColumnCount).map {
          idx => {
            val res = rs.getMetaData.getColumnClassName(idx) match {
              case "java.lang.String" => rs.getString(idx)
              case "java.lang.Boolean" => rs.getBoolean(idx)
              case "java.lang.Long" => rs.getLong(idx)
              case "java.lang.Integer" => rs.getInt(idx)
              case "java.math.BigDecimal" => rs.getDouble(idx)
              case "java.sql.Date" => rs.getDate(idx)
              case "java.sql.Timestamp" => rs.getTimestamp(idx)
              case _ => rs.getString(idx)
            }
            if (rs.wasNull()) null // test wether the value was null
            else res
          }
        }
        c += Row.fromSeq(b)
      }
      val b = spark.sparkContext.makeRDD(c)
      val schema = jdbcMetadataToStructType(rs.getMetaData)

      spark.createDataFrame(b, schema)
    } finally {
      conn.close()
    }
  }

  def jdbcMetadataToStructType(meta: ResultSetMetaData): StructType = {
    StructType((1 to meta.getColumnCount).map {
      idx =>
        meta.getColumnClassName(idx) match {
          case "java.lang.String" => StructField(meta.getColumnLabel(idx), StringType)
          case "java.lang.Boolean" => StructField(meta.getColumnLabel(idx), BooleanType)
          case "java.lang.Integer" => StructField(meta.getColumnLabel(idx), IntegerType)
          case "java.lang.Long" => StructField(meta.getColumnLabel(idx), LongType)
          case "java.math.BigDecimal" => StructField(meta.getColumnLabel(idx), DoubleType)
          case "java.sql.Date" => StructField(meta.getColumnLabel(idx), DateType)
          case "java.sql.Timestamp" => StructField(meta.getColumnLabel(idx), TimestampType)
          case _ => StructField(meta.getColumnLabel(idx), StringType)
        }
    })
  }

  def tableCopy(url: String, tableSrc: String, tableTarg: String, password: String = "", isUnlogged: Boolean = true): Unit = {
    val conn = connOpen(url, password)
    val unlogged = if (isUnlogged) {
      "UNLOGGED"
    } else {
      ""
    }
    val queryCreate = s"""CREATE $unlogged TABLE "$tableTarg" (LIKE "$tableSrc"  INCLUDING DEFAULTS)"""
    val st: PreparedStatement = conn.prepareStatement(queryCreate)
    st.executeUpdate()
    conn.close()
  }

  def tableCreate(url: String, tableTarg: String, schema: StructType, password: String = "", isUnlogged: Boolean = true): Unit = {
    val conn = connOpen(url, password)
    val unlogged = if (isUnlogged) {
      "UNLOGGED"
    } else {
      ""
    }
    val queryCreate = schema.fields.map(f => {
      s""""%s" %s""".format(f.name, toPostgresDdl(f.dataType.catalogString))
    }).mkString(s"""CREATE $unlogged TABLE IF NOT EXISTS "$tableTarg" (""", ",", ");")

    val st: PreparedStatement = conn.prepareStatement(queryCreate)
    st.executeUpdate()
    conn.close()
  }

  def toPostgresDdl(s: String): String = {
    s match {
      case "string" => "text"
      case "double" => "double precision"
      case "decimal(38,18)" => "double precision"
      case "bigint" => "bigint"
      case "int" => "integer"
      case "date" => "date"
      case "timestamp" => "timestamp"
      case "boolean" => "boolean"
      case "array<int>" => "integer[]"
      case "array<bigint>" => "bigint[]"
      case "array<string>" => "text[]"
      case _ => throw new Exception("data type not handled yet:%s".format(s))
    }
  }

  def tableMove(url: String, tableSrc: String, tableTarg: String, password: String = ""): Unit = {
    val conn = connOpen(url, password)
    val queryCreate = s"""ALTER TABLE "$tableSrc" RENAME TO "$tableTarg" """
    val st: PreparedStatement = conn.prepareStatement(queryCreate)
    st.executeUpdate()
    conn.close()
  }

  private def getMinMaxForColumn(spark: SparkSession, url: String, query: String, partitionColumn: String, password: String = ""): (Long, Long) = {
    val min_max_query =
      s"""(SELECT
          |coalesce(cast(min("$partitionColumn") as bigint), 0) as min,
          |coalesce(cast(max("$partitionColumn") as bigint),0) as max
          |FROM $query) AS tmp1""".stripMargin
    val row = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", min_max_query)
      .option("password", passwordFromConn(url, password))
      .load.first
    val lowerBound = row.getLong(0)
    val upperBound = row.getLong(1)
    (lowerBound, upperBound)
  }

  private def getPartitions(spark: SparkSession, lowerBound: Long, upperBound: Long, numPartitions: Int, splitFactor: Int = 1): RDD[(Int, String)] = {
    val length = BigInt(1) + upperBound - lowerBound
    val splitPartitions = numPartitions * splitFactor

    import spark.implicits._
    val partitions = (0 until splitPartitions).map { i =>
      val start = lowerBound + (i * length / splitPartitions)
      val end = lowerBound + ((i + 1) * length / splitPartitions) - 1
      f"between $start AND $end"
    }.zipWithIndex.map(_.swap).toDS.rdd.partitionBy(new ExactPartitioner(numPartitions))
    partitions
  }

  def inputQueryDf(spark: SparkSession, url: String, query: String, numPartitions: Int, partitionColumn: String, password: String = ""): Dataset[Row] = {
    val queryStr = s"($query) as tmp"
    if (partitionColumn != "") {
      // get min and max for partitioning
      val (lowerBound, upperBound) = getMinMaxForColumn(spark, url, queryStr, partitionColumn)
      // get the partitionned dataset from multiple jdbc stmts
      spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", queryStr)
        .option("driver", "org.postgresql.Driver")
        .option("partitionColumn", partitionColumn)
        .option("lowerBound", lowerBound)
        .option("upperBound", upperBound)
        .option("numPartitions", numPartitions)
        .option("fetchsize", 50000)
        .option("password", passwordFromConn(url, password))
        .load
    } else {
      spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", queryStr)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", 50000)
        .option("password", passwordFromConn(url, password))
        .load
    }
  }

  def outputBulk(spark: SparkSession
                 , url: String
                 , table: String
                 , df: Dataset[Row]
                 , path: String
                 , numPartitions: Int = 8
                 , password: String = ""
                 , reindex: Boolean = false
                 , bulkLoadMode: BulkLoadMode = CSV
  ) = {
    bulkLoadMode match  {
      case CSV => outputBulkCsv(spark, url, table, df, path, numPartitions, password, reindex)
      case Stream => outputBulkStream(spark, url, table, df, numPartitions, password, reindex)
    }
  }

  def outputBulkCsv(spark: SparkSession
                    , url: String
                    , table: String
                    , df: Dataset[Row]
                    , path: String
                    , numPartitions: Int = 8
                    , password: String = ""
                    , reindex: Boolean = false
                   ) = {
    try {
      if (reindex)
        indexDeactivate(url, table, password)
      val columns = df.schema.fields.map(x => s"${sanP(x.name)}").mkString(",")
      //transform arrays to string
      val dfTmp = dataframeToPgCsv(spark, df, df.schema)
      //write a csv folder
      dfTmp.write.format("csv")
        .option("delimiter", ",")
        .option("header", "false")
        .option("nullValue", null)
        .option("emptyValue", "\"\"")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("ignoreLeadingWhiteSpace", "false")
        .option("ignoreTrailingWhiteSpace", "false")
        .mode(org.apache.spark.sql.SaveMode.Overwrite)
        .save(path)
      outputBulkCsvLow(spark, url, table, columns, path, numPartitions, ",", ".*.csv", password)
    } finally {
      if (reindex)
        indexReactivate(url, table, password)
    }
  }

  def outputBulkCsvLow(spark: SparkSession
                       , url: String
                       , table: String
                       , columns: String
                       , path: String
                       , numPartitions: Int = 8
                       , delimiter: String = ","
                       , csvPattern: String = ".*.csv"
                       , password: String = ""
                      ) = {

    // load the csv files from hdfs in parallel
    val fs = FileSystem.get(new Configuration())
    import spark.implicits._
    val rdd = fs.listStatus(new Path(path))
      .filter(x => x.getPath.toString.matches("^.*/" + csvPattern + "$"))
      .map(x => x.getPath.toString).toList.zipWithIndex.map(_.swap)
      .toDS.rdd.partitionBy(new ExactPartitioner(numPartitions))

    rdd.foreachPartition(
      x => {
        val conn = connOpen(url, password)
        x.foreach {
          s => {
            val stream = FileSystem.get(new Configuration()).open(new Path(s._2)).getWrappedStream
            val copyManager: CopyManager = new CopyManager(conn.asInstanceOf[BaseConnection])
            copyManager.copyIn( s"""COPY "$table" ($columns) FROM STDIN WITH CSV DELIMITER '$delimiter'  NULL '' ESCAPE '"' QUOTE '"' """, stream)
          }
        }
        conn.close()
        x.toIterator
      })
  }

  def outputBulkStream(spark: SparkSession
                       , url: String
                       , table: String
                       , df: Dataset[Row]
                       , numPartitions: Int = 8
                       , password: String = ""
                       , reindex: Boolean = false
                       , copyTimeoutMs: Long = 1000 * 60 * 10
  ) = {
    try {
      if (reindex)
        indexDeactivate(url, table, password)

      val delimiter = ","
      val schema = df.schema
      val columns = schema.fields.map(x => s"${sanP(x.name)}").mkString(",")
      //transform arrays to string
      val dfTmp = dataframeToPgCsv(spark, df, schema)

      val rddToWrite: RDD[(Long, Row)] = dfTmp.rdd.zipWithIndex.map(_.swap).partitionBy(new ExactPartitioner(numPartitions))

      rddToWrite.foreachPartition((p: Iterator[(Long, Row)]) => {

        val outputStream = new PipedOutputStream()
        val inputStream = new PipedInputStream(outputStream)
        val valueConverters: Array[(Row, Int) => String] = schema.map(s => PGConverter.makeConverter(s.dataType)).toArray

        // Prepare a thread to copy data to PG asynchronously
        val promisedCopy = Promise[Unit]
        val sql = s"""COPY "$table" ($columns) FROM STDIN WITH NULL AS 'NULL' DELIMITER AS E'$delimiter'"""
        val conn = connOpen(url, password)
        val copyManager: CopyManager = new CopyManager(conn.asInstanceOf[BaseConnection])
        val copyThread = new Thread("copy-to-pg-thread") {
          override def run(): Unit = promisedCopy.complete(Try(copyManager.copyIn(sql, inputStream)))
        }

        try {
          // Start the thread
          copyThread.start()
          try {
            // Load the copy stream reading from the partition
            p.foreach { case (_, row) =>
              outputStream.write(PGConverter.convertRow(row, schema.length, delimiter, valueConverters))
            }
            outputStream.close()
            // Wait for the copy to have finished
            Await.result(promisedCopy.future, Duration(copyTimeoutMs, TimeUnit.MILLISECONDS))
          } catch {
            case _: TimeoutException =>
              throw new TimeoutException(
                s"""
                   | The copy operation for copying this partition's data to postgres has been running for
                   | more than the timeout: ${TimeUnit.MILLISECONDS.toSeconds(copyTimeoutMs)}s.
                   | You can configure this timeout with option copyTimeoutMs, such as "2h", "100min",
                   | and default copyTimeout is "10min".
               """.stripMargin)
          }
        } finally {
          // Finalize
          copyThread.interrupt()
          copyThread.join()
          inputStream.close()
          conn.close()
        }

      })


    } finally {
      if (reindex)
        indexReactivate(url, table, password)
    }
  }


  def output(url: String, table: String, df: Dataset[Row], batchsize: Int = 50000, password: String = "") = {
    df.coalesce(8).write.mode(org.apache.spark.sql.SaveMode.Overwrite)
      .format("jdbc")
      .option("url", url)
      .option("dbtable", table)
      .option("batchsize", batchsize)
      .option("password", passwordFromConn(url, password))
      .option("driver", "org.postgresql.Driver")
      .save()
  }

  def inputQueryPartBulkCsv(spark: SparkSession, fsConf: String, url: String, query: String, path: String, numPartitions: Int, partitionColumn: String, splitFactor: Int = 1, password: String = "") = {
    val queryStr = s"($query) as tmp"
    val (lowerBound, upperBound) = getMinMaxForColumn(spark, url, queryStr, partitionColumn)
    val rdd = getPartitions(spark, lowerBound, upperBound, numPartitions, splitFactor)
    rdd.foreachPartition(
      x => {
        val conn = connOpen(url, password)
        x.foreach {
          s => {
            val queryPart = s"""SELECT * FROM $queryStr WHERE "$partitionColumn" ${s._2}"""
            inputQueryBulkCsv(fsConf, conn, queryPart, path)
          }
        }
        conn.close()
        x.toIterator
      })
  }

  def inputQueryBulkCsv(fsConf: String, conn: Connection, query: String, path: String) = {
    val sqlStr = s""" COPY ($query) TO STDOUT  WITH DELIMITER AS ',' CSV NULL '' ENCODING 'UTF-8' QUOTE '"' ESCAPE '"' """
    val copyInputStream: PGCopyInputStream = new PGCopyInputStream(conn.asInstanceOf[BaseConnection], sqlStr)

    val conf = new Configuration()
    conf.set("fs.defaultFS", fsConf)
    val fs = FileSystem.get(conf)
    val output = fs.create(new Path(path, "part-" + randomUUID.toString + ".csv"))

    var flag = true
    while (flag) {
      val t = copyInputStream.read()
      if (t > 0) {
        output.write(t)
        output.write(copyInputStream.readFromCopy())
      } else {
        output.close()
        flag = false
      }
    }
  }

  def getSchema(url: String) = {
    val pattern = "jdbc:postgresql://.+?&currentSchema=(\\w+)".r
    val pattern(schema) = url
    schema
  }

  def indexDeactivate(url: String, table: String, password: String = "") = {
    val schema = getSchema(url)
    val query =
      s"""
    UPDATE pg_index
    SET indisready = false
    WHERE indrelid IN (
    SELECT pg_class.oid FROM pg_class
    JOIN pg_catalog.pg_namespace n ON n.oid = pg_class.relnamespace
    WHERE relname='$table' and nspname = '$schema' )
    """
    sqlExec(url, query, password)
    logger.warn(s"Deactivating indexes from $schema.$table")
  }

  def indexReactivate(url: String, table: String, password: String = "") = {

    val schema = getSchema(url)
    val query =
      s"""
      UPDATE pg_index
      SET indisready = true
      WHERE indrelid IN (
      SELECT pg_class.oid FROM pg_class
      JOIN pg_catalog.pg_namespace n ON n.oid = pg_class.relnamespace
      WHERE relname='$table' and nspname = '$schema' )
    """
    sqlExec(url, query, password)
    logger.warn(s"Reactivating indexes from $schema.$table")

    val query2 =
      s"""
      REINDEX TABLE "$schema"."$table"
    """

    logger.warn(s"Recreating indexes from $schema.$table")
    sqlExec(url, query2, password)
  }

  def inputQueryBulkDf(spark: SparkSession
                       , url: String
                       , query: String
                       , path: String
                       , isMultiline: Boolean = false
                       , numPartitions: Int = 1
                       , partitionColumn: String = ""
                       , splitFactor: Int = 1
                       , password: String = ""
                      ): Dataset[Row] = {
    val defaultFSConf = spark.sessionState.newHadoopConf().get("fs.defaultFS")
    val fsConf = if (path.startsWith("file:")) {
      "file:///"
    } else {
      defaultFSConf
    }

    val conf = new Configuration()
    conf.set("fs.defaultFS", fsConf)
    val fs = FileSystem.get(conf)
    fs.delete(new Path(path), true) // delete file, true for recursive

    val schemaQueryComplex = getSchemaQuery(spark, url, query, password)
    if (numPartitions == 1) {
      val conn = connOpen(url, password)
      inputQueryBulkCsv(fsConf, conn, query, path)
      conn.close()
    } else {
      inputQueryPartBulkCsv(spark, fsConf, url, query, path, numPartitions, partitionColumn, splitFactor, password)
    }

    val schemaQuerySimple = schemaSimplify(schemaQueryComplex)
    // read the resulting csv
    val dfSimple = spark.read.format("csv")
      .schema(schemaQuerySimple)
      .option("multiline", isMultiline)
      .option("delimiter", ",")
      .option("header", "false")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("nullValue", null)
      .option("emptyValue", "\"\"")
      .option("ignoreLeadingWhiteSpace", "false")
      .option("ignoreTrailingWhiteSpace", "false")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .option("dateFormat", "yyyy-MM-dd")
      .option("mode", "FAILFAST")
      .load(path)

    val dfComplex = dataframeFromPgCsv(spark, dfSimple, schemaQueryComplex)
    dfComplex
  }

  def schemaSimplify(schema: StructType): StructType = {
    StructType(schema.fields.map { field =>
      field.dataType match {
        case struct: org.apache.spark.sql.types.BooleanType =>
          field.copy(dataType = org.apache.spark.sql.types.StringType)
        case struct: org.apache.spark.sql.types.ArrayType =>
          field.copy(dataType = org.apache.spark.sql.types.StringType)
        case _ =>
          field
      }
    })
  }

  def sanS(obj: String) = obj.mkString("`", "", "`")

  def sanP(obj: String) = obj.mkString("\"", "", "\"")

  def dataframeFromPgCsv(spark: SparkSession, dfSimple: Dataset[Row], schemaQueryComplex: StructType): Dataset[Row] = {
    val tableTmp = "table_" + randomUUID.toString.replaceAll(".*-", "")
    dfSimple.createOrReplaceTempView(tableTmp)
    val sqlQuery = "SELECT " + schemaQueryComplex.map(a => {
      if (a.dataType.simpleString == "boolean") {
        "CAST(" + sanS(a.name) + " as boolean) as " + sanS(a.name)
      } else if (a.dataType.simpleString.indexOf("array") == 0) {
        "CAST(SPLIT(REGEXP_REPLACE(" + sanS(a.name) + ", '^[{]|[}]$', ''), ',') AS " + a.dataType.simpleString + ") as " + sanS(a.name)
      } else {
        sanS(a.name)
      }
    })
      .mkString(", ") + " FROM " + sanS(tableTmp)
    spark.sql(sqlQuery)
  }

  def dataframeToPgCsv(spark: SparkSession, dfSimple: Dataset[Row], schemaQueryComplex: StructType): DataFrame = {
    val tableTmp = "table_" + randomUUID.toString.replaceAll(".*-", "")
    dfSimple.createOrReplaceTempView(tableTmp)
    val sqlQuery = "SELECT " + schemaQueryComplex.map(a => {
      if (a.dataType.simpleString.indexOf("array") == 0) {
        "REGEXP_REPLACE(REGEXP_REPLACE(CAST(" + sanS(a.name) + " AS string), '^.', '{'), '.$', '}') AS " + sanS(a.name)
      } else if (a.dataType.simpleString.indexOf("string") == 0) {
        "REGEXP_REPLACE(REGEXP_REPLACE(" + sanS(a.name) + ", '\\u0000', ''),'\r\n|\r','\n') AS " + sanS(a.name) // this character breaks postgresql parser
      } else {
        sanS(a.name)
      }

    })
      .mkString(", ") + " FROM " + sanS(tableTmp)
    spark.sql(sqlQuery)
  }


  def tableEmpty(spark: SparkSession, url: String, table: String, password: String): Boolean = {
    val query = s"""select 1 from "$table" limit 1 """
    sqlExecWithResult(spark, url, query, password).count == 0
  }

  def loadEmptyTable(spark: SparkSession, url: String, table: String, candidate: Dataset[Row], path: String, partitions: Int, password: String, bulkLoadMode: BulkLoadMode): Boolean = {
    if (tableEmpty(spark, url, table, password)) {
      logger.warn("Loading directly data")
      outputBulk(spark, url, table, candidate, path, partitions, password, reindex = false, bulkLoadMode)
      return true
    }
    false
  }


  def outputBulkDfScd1Hash(spark: SparkSession
                           , url: String
                           , table: String
                           , candidate: Dataset[Row]
                           , key: List[String]
                           , partitions: Int = 4
                           , path: String
                           , scdFilter: Option[String] = None
                           , deleteSet: Option[String] = None
                           , password: String = ""
                           , bulkLoadMode: BulkLoadMode = CSV
                          ): Unit = {
    if (loadEmptyTable(spark, url, table, candidate, path, partitions, password, bulkLoadMode))
      return

    logger.warn("The postgres table is not empty")

    val insertTmp = getTmpTable("ins_")
    val updateTmp = getTmpTable("upd_")
    val deleteTmp = getTmpTable("del_")
    val isDelete = deleteSet.isDefined

    try {
      // 1. get key/hash
      val queryFetch1 = """select  %s, "%s" from %s where %s""".format(key.mkString("\"", "\",\"", "\""), "hash", sanP(table), scdFilter.getOrElse(" TRUE"))
      val fetch1 = inputQueryBulkDf(spark, url, queryFetch1, path, isMultiline = true, 1, password)

      // 2.1 produce insert
      val joinCol = key.map(x => s"""f.`$x` = c.`$x`""").mkString(" AND ")
      val insert = candidate.as("c").join(fetch1.as("f"), expr(joinCol), "left_anti")
      logger.info(s"Row to insert: ${insert.count}")

      // 2.2 produce insert
      val update = candidate.as("c").join(fetch1.as("f"), expr(joinCol + "AND c.hash != f.hash"), "left_semi")
      logger.info(s"Row to update: ${update.count}")

      // 3. load tmp tables
      tableCreate(url, insertTmp, insert.schema, password)

      outputBulk(spark, url, insertTmp, insert, path + "ins", partitions, password, bulkLoadMode = bulkLoadMode)
      insert.unpersist()

      tableCreate(url, updateTmp, update.schema, password)
      outputBulk(spark, url, updateTmp, update, path + "upd", partitions, password, bulkLoadMode = bulkLoadMode)
      update.unpersist()

      // 4. load postgres
      sqlExec(url, applyScd1(table, insertTmp, updateTmp, insert.schema, key), password)

      // 5 produce delete
      if (isDelete) {
        val delete = fetch1.as("f").join(candidate.as("c"), expr(joinCol), "left_anti")
          .selectExpr(key.mkString("`", "`,`", "`").split(","): _*)
        tableCreate(url, deleteTmp, delete.schema, password)
        outputBulk(spark, url, deleteTmp, delete, path + "del", partitions, password, bulkLoadMode = bulkLoadMode)
        sqlExec(url, applyScd1Delete(table, deleteTmp, key, deleteSet.get), password)
      }

    } finally {
      // 5. drop the temporarytables
      tableDrop(url, insertTmp, password)
      tableDrop(url, updateTmp, password)
      if (isDelete)
        tableDrop(url, deleteTmp, password)
    }
  }

  def applyScd1Delete(table: String, deleteTmp: String, key: List[String], deleteSet: String): String = {
    val joinColumns = key.map(k => s""" t."$k" = s."$k" """).mkString("AND")
    val query =
      s"""
         |UPDATE $table as t
         |SET $deleteSet
         |FROM $deleteTmp as s
         |WHERE ($joinColumns)
         |""".stripMargin
    query

  }

  def applyScd1(table: String, insertTmp: String, updateTmp: String, insertSchema: StructType, key: List[String]): String = {
    val insertCol = insertSchema.fields.map(f => f.name).mkString("\"", "\",\"", "\"")
    val updateCol = insertSchema.fields.map(f => s""" "${f.name}" = s."${f.name}" """).mkString(",")
    val joinColumns = key.map(k => s""" t."$k" = s."$k" """).mkString("AND")
    val query =
      s"""
         |WITH upd as (
         |  UPDATE "$table" as t
         |  SET $updateCol
         |  FROM "$updateTmp" as s
         |  WHERE ($joinColumns)
         |)
         |INSERT INTO "$table" ($insertCol)
         |SELECT $insertCol
         |FROM "$insertTmp"
         |""".stripMargin
    logger.warn(query)
    query
  }


  def getTmpTable(str: String): String = {
    val res = str + randomUUID.toString.replaceAll(".*-", "")
    res
  }

  def applyScd2(table: String, insertTmp: String, updateTmp: String, pk: String, endDatetimeCol: String, insertSchema: StructType): String = {
    val insertCol = insertSchema.fields.map(f => f.name).mkString("\"", "\",\"", "\"")
    val query =
      s"""
         |WITH upd as (
         |  UPDATE "$table" set "$endDatetimeCol" = now() WHERE "$pk" IN (SELECT "$pk" FROM "$updateTmp")
         |)
         |INSERT INTO "$table" ($insertCol, "$endDatetimeCol")
         |SELECT $insertCol, null as "$endDatetimeCol"
         |FROM "$insertTmp"
         |""".stripMargin
    logger.warn(query)
    query
  }

  def outputBulkDfScd2Hash(spark: SparkSession
                           , url: String
                           , table: String
                           , candidate: DataFrame
                           , pk: String
                           , key: List[String]
                           , endDatetimeCol: String
                           , partitions: Int
                           , path: String
                           , password: String = ""
                           , bulkLoadMode: BulkLoadMode = CSV
                          ): Unit = {

    if (loadEmptyTable(spark, url, table, candidate, path, partitions, password, bulkLoadMode))
      return

    val insertTmp = getTmpTable("ins_")
    val updateTmp = getTmpTable("upd_")
    try {
      // 1. get the pk/key/hash
      val queryFetch1 = """select "%s", %s, "%s" from %s where "%s" is null """.format(pk, key.mkString("\"", "\",\"", "\""), "hash", table, endDatetimeCol)
      val fetch1 = inputQueryBulkDf(spark, url, queryFetch1, path, isMultiline = true, partitions, pk, 1, password)

      // 2.1 produce insert and update
      val joinCol = key.map(x => s"""f.`$x` = c.`$x`""").mkString(" AND ")
      val insert = candidate.as("c").join(fetch1.as("f"), expr(joinCol + "AND c.hash = f.hash"), "left_anti")

      // 2.2 produce insert and update
      val update = fetch1.as("f").join(candidate.as("c"), expr(joinCol + "AND c.hash != f.hash"), "left_semi").select(col(pk))

      // 3. load tmp tables
      tableCreate(url, insertTmp, insert.schema, password)
      outputBulk(spark, url, insertTmp, insert, path + "ins", partitions, password, bulkLoadMode = bulkLoadMode)

      tableCreate(url, updateTmp, update.schema, password)
      outputBulk(spark, url, updateTmp, update, path + "upd", partitions, password, bulkLoadMode = bulkLoadMode)

      // 4. load postgres
      sqlExec(url, applyScd2(table, insertTmp, updateTmp, pk, endDatetimeCol, insert.schema), password)

    } finally {
      // 5. drop the temporarytables
      tableDrop(url, insertTmp, password)
      tableDrop(url, updateTmp, password)
    }
  }

}

class ExactPartitioner(partitions: Int) extends Partitioner {

  def getPartition(key: Any): Int = {
    key match {
      case l: Long => math.abs(l.toInt % numPartitions())
      case i: Int => math.abs(i % numPartitions())
      case _ => math.abs(key.hashCode() % numPartitions())
    }
  }

  def numPartitions(): Int = partitions
}

