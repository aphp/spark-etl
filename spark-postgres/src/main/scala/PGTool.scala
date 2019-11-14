package io.frama.parisni.spark.postgres

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSetMetaData}
import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, expr}
import java.util.UUID.randomUUID

import io.frama.parisni.spark.dataframe.DFTool
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.postgresql.copy.{CopyManager, PGCopyInputStream}
import org.postgresql.core.BaseConnection

class PGTool(spark: SparkSession, url: String, tmpPath: String) {

  private var password: String = ""

  def setPassword(pwd: String = ""): PGTool = {
    password = PGTool.passwordFromConn(url, pwd)
    this
  }

  def showPassword(): Unit = {
    println(password)
  }

  private def genPath(): String = {
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
    PGTool.outputBulkCsv(spark, url, table, df, genPath, numPartitions, password, reindex)
    this
  }

  def input(query: String, numPartitions: Int = 1, partitionColumn: String = ""): Dataset[Row] = {
    PGTool.inputQueryDf(spark, url, query, numPartitions, partitionColumn, password)
  }

  def output(table: String, df: Dataset[Row], batchsize: Int = 50000): PGTool = {
    PGTool.output(url, table, df, batchsize, password)
    this
  }

  //  def outputScd1(table: String, key: String, df: Dataset[Row], numPartitions: Int = 4, excludeColumns: List[String] = Nil, includeColumns: List[String]): PGUtil = {
  //    PGUtil.outputBulkDfScd1(spark, url, table, key, df, numPartitions, excludeColumns, includeColumns, genPath, password)
  //    this
  //  }

  def outputScd2Hash(table: String, df: DataFrame, pk: String, key: List[String], endDatetimeCol: String, partitions: Option[Int] = None, multiline: Option[Boolean] = None): Unit = {

    PGTool.outputBulkDfScd2Hash(spark, url, table, df, pk, key, endDatetimeCol, partitions.getOrElse(4), multiline.getOrElse(false), genPath, password)
  }

  def outputScd1Hash(table: String, key: List[String], df: Dataset[Row], numPartitions: Option[Int] = None, hash: Option[String] = None, insertDatetime: Option[String] = None, updateDatetime: Option[String] = None, deleteDatetime: Option[String] = None, isDelete: Boolean = false): PGTool = {

    PGTool.outputBulkDfScd1Hash(spark, url, table, key, df, numPartitions.getOrElse(4), hash.getOrElse("hash"), insertDatetime.getOrElse("insert_datetime"), updateDatetime.getOrElse("update_datetime"), deleteDatetime.getOrElse("delete_datetime"), genPath, isDelete, password)
    this
  }

  //  def outputScd2(table: String, key: String, dateBegin: String, dateEnd: String, df: Dataset[Row], numPartitions: Int = 4, excludeColumns: List[String] = Nil): PGUtil = {
  //    PGUtil.outputBulkDfScd2(spark, url, table, key, dateBegin, dateEnd, df, numPartitions, excludeColumns, genPath, password)
  //    this
  //  }

  def outputBulkCsv(table: String, columns: String, path: String, numPartitions: Int = 8, delimiter: String = ",", csvPattern: String = ".*.csv"): PGTool = {
    PGTool.outputBulkCsvLow(spark, url, table, columns, path, numPartitions, delimiter, csvPattern, password)
    this
  }

  def getSchemaQuery(query: String): StructType = {
    PGTool.getSchemaQuery(spark, url, query, password)
  }

}

object PGTool extends java.io.Serializable with LazyLogging {


  def apply(spark: SparkSession, url: String, tmpPath: String): PGTool = new PGTool(spark, url, tmpPath + "/spark-postgres-" + randomUUID.toString).setPassword("")

  private def dbPassword(hostname: String, port: String, database: String, username: String): String = {
    // Usage: val thatPassWord = dbPassword(hostname,port,database,username)
    // .pgpass file format, hostname:port:database:username:password

    val fs = FileSystem.get(new java.net.URI("file:///"), new Configuration)
    val file = fs.open(new Path(scala.sys.env("HOME"), ".pgpass"))
    val content = Iterator.continually(file.readLine()).takeWhile(_ != null).mkString("\n")
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
    file.close()
    passwd
  }

  def passwordFromConn(url: String, password: String): String = {
    if (!password.isEmpty) {
      return (password)
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

  private def getSchemaTable(spark: SparkSession, url: String, table: String): StructType = {
    val query = s""" select * from $table """
    getSchemaQuery(spark, url, query)
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

  private def getCreateStmtFromSchema(struct: StructType, table: String, excludeColumns: List[String]): String = {
    var columns = ""
    var begin = true
    for (col <- struct) {
      if (!excludeColumns.contains(col.name)) {
        if (!begin)
          columns += ",\n"
        var dataType = col.dataType.sql
        if (dataType == "STRING")
          dataType = "text"
        columns += f"${col.name} ${dataType}"

        if (begin)
          begin = false
      }
    }

    return f"create table $table (\n$columns)"
  }

  def tableTruncate(url: String, table: String, password: String = ""): Unit = {
    val conn = connOpen(url, password)
    val st: PreparedStatement = conn.prepareStatement(s"TRUNCATE TABLE $table")
    st.executeUpdate()
    conn.close()
  }

  def tableDrop(url: String, table: String, password: String = ""): Unit = {
    val conn = connOpen(url, password)
    val st: PreparedStatement = conn.prepareStatement(s"DROP TABLE IF EXISTS $table")
    st.executeUpdate()
    conn.close()
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

      val columnsName = (1 to rs.getMetaData.getColumnCount.toInt).map(
        idx => rs.getMetaData.getColumnLabel(idx)).toList

      import scala.collection.mutable.ListBuffer
      var c = new ListBuffer[Row]()
      while (rs.next()) {
        val b = (1 to rs.getMetaData.getColumnCount.toInt).map {
          idx => {
            val res = rs.getMetaData.getColumnClassName(idx).toString match {
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
      //b.toDF(columnsName.toArray: _*)
    } finally {
      conn.close()
    }
  }

  def jdbcMetadataToStructType(meta: ResultSetMetaData): StructType = {
    StructType((1 to meta.getColumnCount.toInt).map {
      idx =>
        meta.getColumnClassName(idx).toString match {
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
    val queryCreate = s"""CREATE $unlogged TABLE $tableTarg (LIKE $tableSrc  INCLUDING DEFAULTS)"""
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
      "%s %s".format(f.name, toPostgresDdl(f.dataType.typeName))
    }).mkString(s"CREATE $unlogged TABLE IF NOT EXISTS $tableTarg (", ",", ");")

    val st: PreparedStatement = conn.prepareStatement(queryCreate)
    st.executeUpdate()
    conn.close()
  }

  def toPostgresDdl(s: String): String = {
    s match {
      case "string" => "text"
      case "double" => "double precision"
      case "long" => "bigint"
      case "integer" => "integer"
      case "timestamp" => "timestamp"
      case "date" => "date"
      case "boolean" => "boolean"
      case _ => "text" //throw new Exception("data type not handled yet:%s".format(s))
    }
  }

  def tableMove(url: String, tableSrc: String, tableTarg: String, password: String = ""): Unit = {
    val conn = connOpen(url, password)
    val queryCreate = s"""ALTER TABLE $tableSrc RENAME TO $tableTarg"""
    val st: PreparedStatement = conn.prepareStatement(queryCreate)
    st.executeUpdate()
    conn.close()
  }

  private def getMinMaxForColumn(spark: SparkSession, url: String, query: String, partitionColumn: String, password: String = ""): Tuple2[Long, Long] = {
    val min_max_query = s"(SELECT coalesce(cast(min($partitionColumn) as bigint), 0) as min, coalesce(cast(max($partitionColumn) as bigint),0) as max FROM $query) AS tmp1"
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

  private def getPartitions(spark: SparkSession, lowerBound: Long, upperBound: Long, numPartitions: Int, splitFactor: Int = 1): RDD[Tuple2[Int, String]] = {
    val length = BigInt(1) + upperBound - lowerBound
    import spark.implicits._
    val partitions = (0 until numPartitions * splitFactor).map { i =>
      val start = lowerBound + ((i * length) / numPartitions / splitFactor)
      val end = lowerBound + (((i + 1) * length) / numPartitions / splitFactor) - 1
      f"between $start AND $end"
    }.zipWithIndex.map { case (a, index) => (index, a) }.toDS.rdd.partitionBy(new ExactPartitioner(numPartitions))
    partitions
  }

  def inputQueryDf(spark: SparkSession, url: String, query: String, numPartitions: Int, partitionColumn: String, password: String = ""): Dataset[Row] = {
    val queryStr = s"($query) as tmp"
    if (partitionColumn != "") {
      // get min and max for partitioning
      val (lowerBound, upperBound): Tuple2[Long, Long] = getMinMaxForColumn(spark, url, queryStr, partitionColumn)
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
      val columns = df.schema.fields.map(x => s"${x.name}").mkString(",")
      //transform arrays to string
      val dfTmp = dataframeToPgCsv(spark, df, df.schema)
      //write a csv folder
      dfTmp.write.format("csv")
        .option("delimiter", ",")
        .option("header", false)
        .option("nullValue", null)
        .option("emptyValue", "\"\"")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("ignoreLeadingWhiteSpace", false)
        .option("ignoreTrailingWhiteSpace", false)
        .mode(org.apache.spark.sql.SaveMode.Overwrite)
        .save(path)
      outputBulkCsvLow(spark, url, table, columns, path, numPartitions, ",", ".*.csv", password)
    } finally {
      if (reindex)
        indexReactivate(url, table, password)
    }
  }

  def outputBulkCsvLow(spark: SparkSession, url: String, table: String, columns: String, path: String, numPartitions: Int = 8, delimiter: String = ",", csvPattern: String = ".*.csv", password: String = "") = {

    // load the csv files from hdfs in parallel
    val fs = FileSystem.get(new Configuration())
    import spark.implicits._
    val rdd = fs.listStatus(new Path(path))
      .filter(x => x.getPath.toString.matches("^.*/" + csvPattern + "$"))
      .map(x => x.getPath.toString).toList.zipWithIndex.map { case (a, i) => (i, a) }
      .toDS.rdd.partitionBy(new ExactPartitioner(numPartitions))

    rdd.foreachPartition(
      x => {
        val conn = connOpen(url, password)
        x.foreach {
          s => {
            val stream = (FileSystem.get(new Configuration())).open(new Path(s._2)).getWrappedStream
            val copyManager: CopyManager = new CopyManager(conn.asInstanceOf[BaseConnection]);
            copyManager.copyIn(s"""COPY $table ($columns) FROM STDIN WITH CSV DELIMITER '$delimiter'  NULL '' ESCAPE '"' QUOTE '"' """, stream);
          }
        }
        conn.close()
        x.toIterator
      })
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
            val queryPart = s"SELECT * FROM $queryStr WHERE $partitionColumn ${s._2}"
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
        output.write(t);
        output.write(copyInputStream.readFromCopy());
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
      conn.close
    } else {
      inputQueryPartBulkCsv(spark, fsConf, url, query, path, numPartitions, partitionColumn, splitFactor, password)
    }

    val schemaQuerySimple = schemaSimplify(schemaQueryComplex)
    // read the resulting csv
    val dfSimple = spark.read.format("csv")
      .schema(schemaQuerySimple)
      .option("multiline", isMultiline)
      .option("delimiter", ",")
      .option("header", false)
      .option("quote", "\"")
      .option("escape", "\"")
      .option("nullValue", null)
      .option("emptyValue", "\"\"")
      .option("ignoreLeadingWhiteSpace", false)
      .option("ignoreTrailingWhiteSpace", false)
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

  def dataframeFromPgCsv(spark: SparkSession, dfSimple: Dataset[Row], schemaQueryComplex: StructType): Dataset[Row] = {
    val tableTmp = "table_" + randomUUID.toString.replaceAll(".*-", "")
    dfSimple.registerTempTable(tableTmp)
    val sqlQuery = "SELECT " + schemaQueryComplex.map(a => {
      if (a.dataType.simpleString == "boolean") {
        "CAST(" + a.name + " as boolean) as " + a.name
      } else if (a.dataType.simpleString.indexOf("array") == 0) {
        "CAST(SPLIT(REGEXP_REPLACE(" + a.name + ", '^[{]|[}]$', ''), ',') AS " + a.dataType.simpleString + ") as " + a.name
      } else {
        a.name
      }
    })
      .mkString(", ") + " FROM " + tableTmp
    spark.sql(sqlQuery)
  }

  def dataframeToPgCsv(spark: SparkSession, dfSimple: Dataset[Row], schemaQueryComplex: StructType): Dataset[Row] = {
    val tableTmp = "table_" + randomUUID.toString.replaceAll(".*-", "")
    dfSimple.registerTempTable(tableTmp)
    val sqlQuery = "SELECT " + schemaQueryComplex.map(a => {
      if (a.dataType.simpleString.indexOf("array") == 0) {
        "REGEXP_REPLACE(REGEXP_REPLACE(CAST(" + a.name + " AS string), '^.', '{'), '.$', '}') AS " + a.name
      } else {
        a.name
      }
    })
      .mkString(", ") + " FROM " + tableTmp
    spark.sql(sqlQuery)
  }

  //  def outputBulkDfScd1(spark: SparkSession, url: String, table: String, key: String, df: Dataset[Row], numpartitions: Int = 8, excludeColumns: List[String] = Nil, includeColumns: List[String] = Nil, path: String, password: String = ""): Unit = {
  //    val tableTmp = "table_" + randomUUID.toString.replaceAll(".*-", "")
  //    tableDrop(url, tableTmp, password)
  //    tableCopy(url, table, tableTmp, password)
  //    outputBulkCsv(spark, url, tableTmp, df, path, numpartitions, password)
  //    scd1Update(url, table, tableTmp, key, df.schema, excludeColumns, includeColumns, false, password)
  //    scd1Insert(url, table, tableTmp, key, df.schema, excludeColumns, includeColumns, password)
  //    tableDrop(url, tableTmp, password)
  //  }

  def outputBulkDfScd1Hash(spark: SparkSession
                           , url: String
                           , table: String
                           , key: List[String]
                           , df: Dataset[Row]
                           , numPartitions: Int = 8
                           , hash: String
                           , insertDatetime: String
                           , updateDatetime: String
                           , deleteDatetime: String
                           , path: String
                           , isDelete: Boolean
                           , password: String = ""): Unit = {
    // fetch both ID and HASH from the entire target table to spark
    // compare ID and HASH from target table and source table:
    //     create new rows
    //     create rows to update
    // insert new rows directly
    // insert rows to update in a temp table, and update
    // df.cache
    // val selectColumns = key.map(x => s"f.${x}").mkString(", ")
    val selectColumnsBasic = key.mkString(", ")
    val joinColumns = key.map(x => s"df.${x} = f.${x}").mkString(" AND ")

    val rand = randomUUID.toString.replaceAll(".*-", "")
    val tableUpdTmp = "table_upd_" + rand
    //val tableDelTmp = "table_del_" + rand
    tableCreate(url, tableUpdTmp, df.schema, password)
    //tableCopy(url, table, tableUpdTmp, password)
    //sqlExec(url = url, query = getCreateStmtFromSchema(getSchemaQuery(spark, url, query = f"select $selectColumnsBasic from $table", password), tableDelTmp, excludeColumns = Nil), password = password)
    df.registerTempTable(f"df_$rand")

    // the hash/pk of the table
    val query = f"select $selectColumnsBasic, $hash from $table"
    val fetch = inputQueryBulkDf(spark, url, query, path, isMultiline = true, numPartitions = 1, password = password)
    fetch.registerTempTable(f"fetch_$rand")

    //val insertDate = if (!isDelete) {
    //  f", current_timestamp as $insertDatetime"
    //} else {
    //  ""
    //}
    //val updateDate = if (!isDelete) {
    //  f", current_timestamp as $updateDatetime"
    //} else {
    //  ""
    //}

    val insertDate = ""
    val updateDate = ""

    // we insert the rows that are not yet present
    val insDf = spark.sql(f"select df.* $insertDate from df_$rand df left join fetch_$rand f  on ($joinColumns) where f.${key(0)} is null")

    val updDf = spark.sql(f"select df.* $updateDate from df_$rand df        join fetch_$rand f          on ($joinColumns) where f.$hash IS DISTINCT FROM df.$hash")

    // to UPDATE -> table update (all columns but
    outputBulkCsv(spark, url, tableUpdTmp, updDf, path + "/upd", numPartitions, password)
    scd1Update(url, table, tableUpdTmp, key, df.schema, excludeColumns = Nil, includeColumns = Nil, isCompare = false, password)

    outputBulkCsv(spark, url, table, insDf, path + "/ins", numPartitions, password)
    tableDrop(url, tableUpdTmp, password)

    //val delDf = spark.sql(f"select $selectColumns                             from fetch_$rand f  left join  df_$rand df          on ($joinColumns) where df.$hash is null")
    //outputBulkCsv(spark, url, tableDelTmp, delDf, path + "/del", numPartitions, password)
    //if (isDelete) {
    //  // the rows are deleted
    //  sqlExec(url = url, query = f"delete from $table df using $tableDelTmp f where $joinColumns", password = password)
    //} else {
    //  // we tag the row as deleted
    //  sqlExec(url = url, query = f"update $table df set $deleteDatetime = now() from $tableDelTmp f where $joinColumns", password = password)
    //}
    // insert at the end to lighter the update and delete part
    //tableDrop(url, tableDelTmp, password)
  }

  def scd1Update(url: String, table: String, tableTarg: String, key: List[String], rddSchema: StructType, excludeColumns: List[String] = Nil, includeColumns: List[String] = Nil, isCompare: Boolean = true, password: String = ""): Unit = {
    val conn = connOpen(url, password)
    val joinColumns = key.map(x => s"targ.${x} = tmp.${x}").mkString(" AND ")
    val updSet = rddSchema.fields.filter(x => !key.contains(x.name)).filter(x => !excludeColumns.contains(x.name)).map(x => s"${x.name} = tmp.${x.name}").mkString(",")
    var updIsDistinct = rddSchema.fields.filter(x => !key.contains(x.name)).filter(x => !excludeColumns.contains(x.name)).map(x => s"tmp.${x.name} IS DISTINCT FROM tmp.${x.name}").mkString(" OR ")
    if (includeColumns.size != 0) { //Either exclude or include
      updIsDistinct = includeColumns.map(x => s"tmp.${x} IS DISTINCT FROM tmp.${x}").mkString(" OR ")
    }
    if (!isCompare) { //update in any case
      updIsDistinct = "1 = 1"
    }

    val upd =
      s"""
    UPDATE $table as targ
    SET $updSet
    FROM $tableTarg as tmp
    WHERE TRUE
    AND ($joinColumns)
    AND ($updIsDistinct)
    """
    conn.prepareStatement(upd).executeUpdate()

    conn.close()
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
                           , df: DataFrame
                           , pk: String
                           , key: List[String]
                           , endDatetimeCol: String
                           , partitions: Int
                           , multiline: Boolean
                           , path: String
                           , password: String = ""
                          ): Unit = {
    val candidate = DFTool.dfAddHash(df)
    val insertTmp = getTmpTable("ins_")
    val updateTmp = getTmpTable("upd_")
    try {
      // 1. get the pk/key/hash
      val queryFetch1 = """select "%s", %s, "%s" from %s where "%s" is null """.format(pk, key.mkString("\"", "\",\"", "\""), "hash", table, endDatetimeCol)
      val fetch1 = inputQueryBulkDf(spark, url, queryFetch1, path, multiline, partitions, pk, 1, password)

      // 2.1 produce insert and update
      val joinCol = key.map(x => s"""f.`$x` = c.`$x`""").mkString(" AND ")
      val insert = DFTool.dfAddHash(candidate).as("c").join(fetch1.as("f"), expr(joinCol + "AND c.hash = f.hash"), "left_anti")

      // 2.2 produce insert and update
      val update = fetch1.as("f").join(candidate.as("c"), expr(joinCol + "AND c.hash != f.hash"), "left_semi").select(col(pk))

      // 3. load tmp tables
      tableCreate(url, insertTmp, insert.schema, password)
      outputBulkCsv(spark, url, insertTmp, insert, path + "ins", partitions, password)

      tableCreate(url, updateTmp, update.schema, password)
      outputBulkCsv(spark, url, updateTmp, update, path + "upd", partitions, password)


      // 4. load postgres
      sqlExec(url, applyScd2(table, insertTmp, updateTmp, pk, endDatetimeCol, insert.schema), password)

    } finally {
      // 5. drop the temporarytables
      tableDrop(url, insertTmp, password)
      tableDrop(url, updateTmp, password)
    }
  }

  //  def scd1Insert(url: String, table: String, tableTarg: String, key: String, rddSchema: StructType, excludeColumns: List[String] = Nil, includeColumns: List[String] = Nil, password: String = ""): Unit = {
  //    val conn = connOpen(url, password)
  //
  //    val insSet = rddSchema.fields.filter(x => !excludeColumns.contains(x.name)).map(x => s"${x.name}").mkString(",")
  //    val insSetTarg = rddSchema.fields.filter(x => !excludeColumns.contains(x.name)).map(x => s"tmp.${x.name}").mkString(",")
  //
  //    val ins = s"""
  //    INSERT INTO $table ($insSet)
  //    SELECT $insSetTarg
  //    FROM $tableTarg as tmp
  //    LEFT JOIN $table as targ USING ($key)
  //    WHERE TRUE
  //    AND targ.$key IS NULL
  //    """
  //    conn.prepareStatement(ins).executeUpdate()
  //    conn.close()
  //  }

  //  def outputBulkDfScd2(spark: SparkSession, url: String, table: String, key: String, dateBegin: String, dateEnd: String, df: Dataset[Row], numPartitions: Int = 8, excludeColumns: List[String] = Nil, path: String, password: String = ""): Unit = {
  //    val tableTmp = "table_" + randomUUID.toString.replaceAll(".*-", "")
  //    tableDrop(url, tableTmp, password)
  //    tableCopy(url, table, tableTmp, password)
  //    outputBulkCsv(spark, url, tableTmp, df, path, numPartitions, password)
  //    scd2(url, table, tableTmp, key, dateBegin, dateEnd, df.schema, excludeColumns, password)
  //    tableDrop(url, tableTmp, password)
  //  }

  //  def scd2(url: String, table: String, tableTmp: String, key: String, dateBegin: String, dateEnd: String, rddSchema: StructType, excludeColumns: List[String] = Nil, password: String = ""): Unit = {
  //    val conn = connOpen(url, password)
  //    val insCols = rddSchema.fields.filter(x => x.name != dateBegin).filter(x => !excludeColumns.contains(x.name)).map(x => s"${x.name}").mkString(",") + "," + dateBegin
  //    val insColsTmp = rddSchema.fields.filter(x => x.name != dateBegin).filter(x => !excludeColumns.contains(x.name)).map(x => s"tmp.${x.name}").mkString(",") + ", now()"
  //
  //    // update  where key AND  is distinct from
  //    // insert  where key AND  is distinct from
  //    val oldRows = s"""
  //    WITH
  //    upd AS (
  //    UPDATE $table c
  //    SET $dateEnd = now()
  //    FROM $tableTmp tmp
  //    WHERE TRUE
  //    AND tmp.$key  = c.$key
  //    AND c.$dateEnd IS null
  //    AND tmp.concept_code IS DISTINCT FROM c.concept_code
  //    RETURNING c.$key
  //    )
  //    INSERT INTO $table ($insCols)
  //    SELECT $insColsTmp
  //    FROM $tableTmp tmp
  //    JOIN upd USING ($key)
  //    """
  //    conn.prepareStatement(oldRows).executeUpdate()
  //
  //    // insert  where key AND left join null
  //    val newRows = s"""
  //    INSERT INTO $table ($insCols)
  //    SELECT $insColsTmp
  //    FROM $tableTmp tmp
  //    LEFT JOIN $table c USING ($key)
  //    WHERE c.$key IS null;
  //    """
  //    conn.prepareStatement(newRows).executeUpdate()
  //    conn.close()
  //  }
}

class ExactPartitioner[V](partitions: Int) extends Partitioner {
  def getPartition(key: Any): Int = return math.abs(key.asInstanceOf[Int] % numPartitions())

  def numPartitions(): Int = partitions
}
