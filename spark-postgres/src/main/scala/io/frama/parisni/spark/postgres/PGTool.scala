package io.frama.parisni.spark.postgres

import java.io._
import java.sql.{
  Connection,
  Date,
  DriverManager,
  PreparedStatement,
  ResultSetMetaData,
  Timestamp
}
import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit
import java.util.{Properties, TimeZone}

import com.typesafe.scalalogging.LazyLogging
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils
import io.frama.parisni.spark.postgres.rowconverters._
import org.apache.commons.codec.Charsets
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.postgresql.copy.{CopyManager, PGCopyInputStream, PGCopyOutputStream}
import org.postgresql.core.BaseConnection

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Promise, TimeoutException}
import scala.util.{Failure, Success, Try}

sealed trait BulkLoadMode

object CSV extends BulkLoadMode

object Stream extends BulkLoadMode

object PgBinaryStream extends BulkLoadMode

object PgBinaryFiles extends BulkLoadMode

class PGTool(spark: SparkSession,
             url: String,
             tmpPath: String,
             bulkLoadMode: BulkLoadMode,
             bulkLoadBufferSize: Int) {

  private var password: String = ""

  def setPassword(pwd: String = ""): PGTool = {
    password = PGTool.passwordFromConn(url, pwd)
    this
  }

  def showPassword(): Unit = {
    println(password)
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

  def tableExists(table: String): Boolean = {
    PGTool.tableExists(url, table)
  }

  /**
    * Copy a table from an other table excluding data
    *
    * Takes parameters to copy constraints, indexes, storage, comments, owner, permissions
    *
    * Limitations:
    *  - If defaults use functions, the copied table and source table will be using the same function
    * and might be functionally linked
    *  - reloptions are not copied
    */
  def tableCopy(tableSrc: String,
                tableTarg: String,
                isUnlogged: Boolean = true,
                copyConstraints: Boolean = false,
                copyIndexes: Boolean = false,
                copyStorage: Boolean = false,
                copyComments: Boolean = false,
                copyOwner: Boolean = false,
                copyPermissions: Boolean = false): PGTool = {
    PGTool.tableCopy(url,
                     tableSrc,
                     tableTarg,
                     password,
                     isUnlogged,
                     copyConstraints,
                     copyIndexes,
                     copyStorage,
                     copyComments,
                     copyOwner,
                     copyPermissions)
    this
  }

  def tableCreate(tableTarg: String,
                  schema: StructType,
                  isUnlogged: Boolean = true): PGTool = {
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

  def sqlExec(query: String, params: List[Any] = Nil): PGTool = {
    PGTool.sqlExec(url, query, password, params)
    this
  }

  def sqlExecWithResult(query: String,
                        params: List[Any] = Nil): Dataset[Row] = {
    PGTool.sqlExecWithResult(spark, url, query, password, params)
  }

  /**
    * Get a spark dataframe from a postgres SQL using
    * the built-in COPY
    *
    * @return DataFrame
    *
    */
  def inputBulk(query: String,
                isMultiline: Option[Boolean] = None,
                numPartitions: Option[Int] = None,
                splitFactor: Option[Int] = None,
                partitionColumn: String = ""): Dataset[Row] = {
    PGTool.inputQueryBulkDf(spark,
                            url,
                            query,
                            genPath,
                            isMultiline.getOrElse(false),
                            numPartitions.getOrElse(1),
                            partitionColumn,
                            splitFactor.getOrElse(1),
                            password = password)
  }

  /**
    * Write a spark dataframe into a postgres table using
    * the built-in COPY
    *
    */
  def outputBulk(table: String,
                 df: Dataset[Row],
                 numPartitions: Option[Int] = None,
                 reindex: Boolean = false): PGTool = {
    PGTool.outputBulk(spark,
                      url,
                      table,
                      df,
                      genPath,
                      numPartitions.getOrElse(8),
                      password,
                      reindex,
                      bulkLoadMode,
                      bulkLoadBufferSize)
    this
  }

  def input(query: String,
            numPartitions: Int = 1,
            partitionColumn: String = ""): Dataset[Row] = {
    PGTool.inputQueryDf(spark,
                        url,
                        query,
                        numPartitions,
                        partitionColumn,
                        password)
  }

  def output(table: String,
             df: Dataset[Row],
             batchsize: Int = 50000): PGTool = {
    PGTool.output(url, table, df, batchsize, password)
    this
  }

  def outputScd2Hash(table: String,
                     df: DataFrame,
                     pk: String,
                     key: Seq[String],
                     endDatetimeCol: String,
                     numPartitions: Option[Int] = None,
                     multiline: Option[Boolean] = None): Unit = {

    PGTool.outputBulkDfScd2Hash(spark,
                                url,
                                table,
                                df,
                                pk,
                                key,
                                endDatetimeCol,
                                numPartitions.getOrElse(4),
                                genPath,
                                password,
                                bulkLoadMode,
                                bulkLoadBufferSize)
  }

  private def genPath: String = {
    tmpPath + "/" + randomUUID.toString
  }

  def outputScd1Hash(table: String,
                     key: Seq[String],
                     df: Dataset[Row],
                     numPartitions: Option[Int] = None,
                     filter: Option[String] = None,
                     deleteSet: Option[String] = None): PGTool = {

    PGTool.outputBulkDfScd1Hash(spark,
                                url,
                                table,
                                df,
                                key,
                                numPartitions.getOrElse(4),
                                genPath,
                                filter,
                                deleteSet,
                                password,
                                bulkLoadMode,
                                bulkLoadBufferSize)
    this
  }

  def outputBulkCsv(table: String,
                    columns: String,
                    path: String,
                    numPartitions: Int = 8,
                    delimiter: String = ",",
                    csvPattern: String = ".*.csv"): PGTool = {

    PGTool.outputBulkCsvLow(spark,
                            url,
                            table,
                            columns,
                            path,
                            numPartitions,
                            delimiter,
                            csvPattern,
                            password)
    this
  }

  def getSchemaQuery(query: String): StructType = {
    PGTool.getSchemaQuery(spark, url, query, password)
  }

  def getConnection(): Connection = {
    PGTool.connOpen(url, password)
  }

}

object PGTool extends java.io.Serializable with LazyLogging {

  val defaultBulkLoadStrategy: BulkLoadMode = CSV
  val defaultBulkLoadBufferSize = 512 * 1024
  val defaultStreamBulkLoadTimeoutMs = 10 * 64 * 1000

  def apply(
      spark: SparkSession,
      url: String,
      tmpPath: String,
      bulkLoadMode: BulkLoadMode = defaultBulkLoadStrategy,
      bulkLoadBufferSize: Int = defaultBulkLoadBufferSize
  ): PGTool = {
    new PGTool(spark,
               url,
               tmpPath + "/spark-postgres-" + randomUUID.toString,
               bulkLoadMode,
               bulkLoadBufferSize).setPassword("")
  }

  def tableExists(url: String,
                  table: String,
                  password: String = ""): Boolean = {
    val conn = connOpen(url, password)
    val st: PreparedStatement = conn.prepareStatement(s"""
         |SELECT EXISTS (
         |  SELECT 1
         |  FROM information_schema.tables
         |	WHERE table_name = '$table'
         |)""".stripMargin)
    val rs = st.executeQuery()
    rs.next()
    val res = rs.getBoolean(1)
    conn.close()
    res
  }

  def tableTruncate(url: String, table: String, password: String = ""): Unit = {
    val conn = connOpen(url, password)
    val st: PreparedStatement =
      conn.prepareStatement(s"""TRUNCATE TABLE "$table" """)
    st.executeUpdate()
    conn.close()
  }

  def connOpen(url: String, password: String = ""): Connection = {
    val prop = new Properties()
    prop.put("password", passwordFromConn(url, password))
    val dbc: Connection = DriverManager.getConnection(url, prop)
    dbc
  }

  def passwordFromConn(url: String, password: String): String = {
    if (!password.isEmpty) {
      return password
    }
    val pattern = "jdbc:postgresql://(.*):(\\d+)/(\\w+)[?]user=(\\w+).*".r
    val pattern(host, port, database, username) = url
    dbPassword(host, port, database, username)
  }

  private def dbPassword(hostname: String,
                         port: String,
                         database: String,
                         username: String): String = {
    // Usage: val thatPassWord = dbPassword(hostname,port,database,username)
    // .pgpass file format, hostname:port:database:username:password

    val fs = FileSystem.get(new java.net.URI("file:///"), new Configuration)
    val reader = new BufferedReader(
      new InputStreamReader(
        fs.open(new Path(scala.sys.env("HOME"), ".pgpass"))))
    val content = Iterator
      .continually(reader.readLine())
      .takeWhile(_ != null)
      .mkString("\n")
    var passwd = ""
    content.split("\n").foreach { line =>
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

  def tableDrop(url: String, table: String, password: String = ""): Unit = {
    val conn = connOpen(url, password)
    val st: PreparedStatement =
      conn.prepareStatement(s"""DROP TABLE IF EXISTS "$table" """)
    st.executeUpdate()
    conn.close()
  }

  def tableRename(url: String,
                  from: String,
                  to: String,
                  password: String = ""): Unit = {
    val conn = connOpen(url, password)
    val st: PreparedStatement =
      conn.prepareStatement(s"""ALTER TABLE "$from" RENAME TO "$to" """)
    st.executeUpdate()
    conn.close()
  }

  def killLocks(url: String, table: String, password: String): Int = {
    logger.warn(s"Looking for locks")
    var killed = 0
    val conn = connOpen(url, password)
    try {
      val st = conn.prepareStatement(
        "select pid from pg_locks l join pg_class t on l.relation = t.oid and t.relkind = 'r' where t.relname = ?")
      st.setString(1, table)
      val rs = st.executeQuery()
      val st2 = conn.prepareStatement(
        "select pg_cancel_backend(?) AND pg_terminate_backend(?)")

      while (rs.next()) {
        val pid = rs.getInt("pid")
        st2.setInt(1, pid)
        st2.setInt(2, pid)
        st2.executeQuery()
        killed += 1
        logger.warn(s"Killed postgres process $pid")
      }
    } finally {
      conn.close()
    }
    killed
  }

  def sqlExec(url: String,
              query: String,
              password: String = "",
              params: List[Any] = Nil): Unit = {
    val conn = connOpen(url, password)
    val st: PreparedStatement = conn.prepareStatement(s"$query")
    parametrize(st, params).executeUpdate()
    conn.close()
  }

  def copyTableOwner(url: String,
                     tableSrc: String,
                     tableTarg: String,
                     password: String = ""): Unit = {
    val conn = connOpen(url, password)

    val retrieveTablePermsQuery =
      s"""
         |SELECT r.rolname
         |FROM pg_roles r
         |  JOIN pg_class c
         |    ON r.oid = c.relowner
         |WHERE c.relname = '$tableSrc'
         |  AND c.relkind = 'r' -- Only consider ordinary tables
       """.stripMargin
    val retrieveOwnerStatement: PreparedStatement =
      conn.prepareStatement(retrieveTablePermsQuery)
    val ownerResultSet = retrieveOwnerStatement.executeQuery()
    // If no result, throw error
    if (ownerResultSet.isAfterLast)
      throw new IllegalStateException(s"No owner to copy for table $tableSrc")

    // Retrieve owner
    ownerResultSet.next()
    val owner = ownerResultSet.getString(1)

    // Throw error if more than one row (multiple rows for same table-name)
    ownerResultSet.next()
    if (!ownerResultSet.isAfterLast)
      throw new IllegalStateException(
        s"More than one owner row for table $tableSrc")
    ownerResultSet.close()
    retrieveOwnerStatement.close()

    // Update owner of taget table
    val updateOwnerStatement: PreparedStatement =
      conn.prepareStatement(s"ALTER TABLE $tableTarg OWNER TO $owner")
    updateOwnerStatement.execute()
    updateOwnerStatement.close()

    conn.close()

  }

  def postgresPermissionToGrant(permission: String,
                                role: String,
                                tableTarg: String,
                                column: Option[String]): String = {
    // No role means permission is assigned to all
    val roleSql = if (role.isEmpty) "PUBLIC" else role

    // If permission has 2 chars, second char is * meaning add grant option to perm
    val grantOption = if (permission.length == 2) "WITH GRANT OPTION" else ""

    val columnSql = column.map(name => s"($name)").getOrElse("")

    val privilege = permission.head match {
      case 'a' => "INSERT"
      case 'r' => "SELECT"
      case 'w' => "UPDATE"
      case 'd' => "DELETE"
      case 'D' => "TRUNCATE"
      case 'x' => "REFERENCES"
      case 't' => "TRIGGER"
    }
    s"GRANT $privilege $columnSql ON $tableTarg TO $roleSql $grantOption"
  }

  def postgresPermissionsArrayToGrants(permissions: String,
                                       tableTarg: String,
                                       column: Option[String]): Seq[String] = {
    val permsPattern = "^\\{(\\S*=([arwdDxt]\\\\*?)+/\\S*)+\\}$".r
    if (permissions != null) {
      permissions match {
        case permsPattern(_*) =>
          val permissionsSplit = permissions.drop(1).dropRight(1).split(",")
          permissionsSplit.flatMap(rolePerms => {
            val Array(role, perms) = rolePerms.split("/")(0).split("=")
            val matches = "([arwdDxt]\\*?)".r.findAllIn(perms)
            matches.map(perm => {
              postgresPermissionToGrant(perm, role, tableTarg, column)
            })
          })
        case _ =>
          throw new IllegalStateException(
            s"Incorrect permissions-array format for $permissions")
      }
    } else {
      Seq.empty
    }
  }

  def copyTablePermissions(url: String,
                           tableSrc: String,
                           tableTarg: String,
                           password: String = ""): Unit = {
    val conn = connOpen(url, password)

    val retrieveTablePermsQuery =
      s"""
         |SELECT relacl
         |FROM pg_class
         |WHERE relname = '$tableSrc'
         |  AND relkind = 'r' -- Only consider ordinary tables
       """.stripMargin
    val retrieveTableSrcPermsStatement: PreparedStatement =
      conn.prepareStatement(retrieveTablePermsQuery)
    val tableSrcPermsResultSet = retrieveTableSrcPermsStatement.executeQuery()
    // If no result, throw error
    if (tableSrcPermsResultSet.isAfterLast)
      throw new IllegalStateException(
        s"No permission to copy for table $tableSrc")

    // Retrieve permissions as postgres array-string
    tableSrcPermsResultSet.next()
    val tableSrcPermsToParse = tableSrcPermsResultSet.getString(1)

    // Throw error if more than one row (multiple rows for same table-name
    tableSrcPermsResultSet.next()
    if (!tableSrcPermsResultSet.isAfterLast)
      throw new IllegalStateException(
        s"More than one permission row for table $tableSrc")
    tableSrcPermsResultSet.close()
    retrieveTableSrcPermsStatement.close()

    // Parse permissions and apply them
    postgresPermissionsArrayToGrants(tableSrcPermsToParse,
                                     tableTarg,
                                     column = None).foreach(grant => {
      val updateTableTargPermsStatement: PreparedStatement =
        conn.prepareStatement(grant)
      updateTableTargPermsStatement.execute()
      updateTableTargPermsStatement.close()
    })

    conn.close()

  }

  def copyColumnsPermissions(url: String,
                             tableSrc: String,
                             tableTarg: String,
                             password: String = ""): Unit = {
    val conn = connOpen(url, password)

    val retrieveTableSrcColumnsPermsQuery =
      s"""
         |SELECT a.attname, a.attacl
         |FROM pg_attribute a
         |  JOIN pg_class c
         |    ON a.attrelid = c.oid
         |WHERE c.relname = '$tableSrc'
         |  AND c.relkind = 'r' -- Only consider ordinary tables
         |  -- Look only to attribute explicitly defined (no internal one)
         |  AND a.attname IN (
         |    SELECT column_name
         |    FROM information_schema.columns
         |    WHERE table_name   = '$tableSrc')
       """.stripMargin
    val retrieveTableSrcColumnsPermsStatement: PreparedStatement =
      conn.prepareStatement(retrieveTableSrcColumnsPermsQuery)
    val tableSrcColumnsPermsResultSet =
      retrieveTableSrcColumnsPermsStatement.executeQuery()

    // if some result, loop over them
    if (!tableSrcColumnsPermsResultSet.isAfterLast) {
      tableSrcColumnsPermsResultSet.next()
      while (!tableSrcColumnsPermsResultSet.isAfterLast) {
        // Retrieve column-name and permissions as postgres array-string
        val columnName = tableSrcColumnsPermsResultSet.getString(1)
        val columnPermsToParse = tableSrcColumnsPermsResultSet.getString(2)

        // Parse permissions and apply them
        postgresPermissionsArrayToGrants(
          columnPermsToParse,
          tableTarg,
          column = Some(columnName)).foreach(grant => {
          val updateTableTargColumnPermsStatement: PreparedStatement =
            conn.prepareStatement(grant)
          updateTableTargColumnPermsStatement.execute()
          updateTableTargColumnPermsStatement.close()
        })

        tableSrcColumnsPermsResultSet.next()
      }
    }
    tableSrcColumnsPermsResultSet.close()
    conn.close()

  }

  def tableCopy(url: String,
                tableSrc: String,
                tableTarg: String,
                password: String = "",
                isUnlogged: Boolean = true,
                copyConstraints: Boolean = false,
                copyIndexes: Boolean = false,
                copyStorage: Boolean = false,
                copyComments: Boolean = false,
                copyOwner: Boolean = false,
                copyPermissions: Boolean = false): Unit = {
    val conn = connOpen(url, password)
    val unlogged = if (isUnlogged) "UNLOGGED" else ""
    // TODO -- Add triggers
    val copyIncludeParams = {
      Seq("INCLUDING DEFAULTS") ++
        (if (copyConstraints) Seq("INCLUDING CONSTRAINTS") else Nil) ++
        (if (copyIndexes) Seq("INCLUDING INDEXES") else Nil) ++
        (if (copyStorage) Seq("INCLUDING STORAGE") else Nil) ++
        (if (copyComments) Seq("INCLUDING COMMENTS") else Nil)
    }

    val queryCreate =
      s"""CREATE $unlogged TABLE "$tableTarg" (LIKE "$tableSrc" ${copyIncludeParams
        .mkString(" ")})"""
    val st: PreparedStatement = conn.prepareStatement(queryCreate)
    st.executeUpdate()
    conn.close()

    if (copyPermissions) {
      copyTablePermissions(url, tableSrc, tableTarg, password)
      copyColumnsPermissions(url, tableSrc, tableTarg, password)
    }

    // Copy owner last to prevent permissions issues
    if (copyOwner) {
      copyTableOwner(url, tableSrc, tableTarg)
    }

  }

  def tableCreate(url: String,
                  tableTarg: String,
                  schema: StructType,
                  password: String = "",
                  isUnlogged: Boolean = true): Unit = {
    val conn = connOpen(url, password)
    val unlogged = if (isUnlogged) {
      "UNLOGGED"
    } else {
      ""
    }
    val queryCreate = schema.fields
      .map(f => {
        s""""%s" %s""".format(f.name, toPostgresDdl(f.dataType.catalogString))
      })
      .mkString(s"""CREATE $unlogged TABLE IF NOT EXISTS "$tableTarg" (""",
                ",",
                ");")

    val st: PreparedStatement = conn.prepareStatement(queryCreate)
    st.executeUpdate()
    conn.close()
  }

  def toPostgresDdl(s: String): String = {
    val mapStructPattern = "^(map|struct)<(.*)>$".r

    s match {
      case "boolean" => "boolean"
      // No tinyint in postgres - using smallint instead
      case "tinyint"        => "smallint"
      case "smallint"       => "smallint"
      case "int"            => "integer"
      case "bigint"         => "bigint"
      case "float"          => "float4"
      case "double"         => "double precision"
      case "decimal(38,18)" => "double precision"
      case "string"         => "text"
      case "date"           => "date"
      case "timestamp"      => "timestamp"
      case "binary"         => "bytea"

      case "array<boolean>"        => "boolean[]"
      case "array<tinyint>"        => "smallint[]"
      case "array<smallint>"       => "smallint[]"
      case "array<int>"            => "integer[]"
      case "array<bigint>"         => "bigint[]"
      case "array<float>"          => "float4[]"
      case "array<double>"         => "double precision[]"
      case "array<decimal(38,18)>" => "double precision[]"
      case "array<string>"         => "text[]"
      case "array<date>"           => "date[]"
      case "array<timestamp>"      => "timestamp[]"
      case "array<binary>"         => "bytea[]"

      case mapStructPattern(_, _) => "jsonb"

      case _ => throw new Exception("data type not handled yet:%s".format(s))
    }
  }

  def tableMove(url: String,
                tableSrc: String,
                tableTarg: String,
                password: String = ""): Unit = {
    val conn = connOpen(url, password)
    val queryCreate = s"""ALTER TABLE "$tableSrc" RENAME TO "$tableTarg" """
    val st: PreparedStatement = conn.prepareStatement(queryCreate)
    st.executeUpdate()
    conn.close()
  }

  def inputQueryDf(spark: SparkSession,
                   url: String,
                   query: String,
                   numPartitions: Int,
                   partitionColumn: String,
                   password: String = ""): Dataset[Row] = {
    val queryStr = s"($query) as tmp"
    if (partitionColumn != "") {
      // get min and max for partitioning
      val (lowerBound, upperBound) =
        getMinMaxForColumn(spark, url, queryStr, partitionColumn)
      // get the partitionned dataset from multiple jdbc stmts
      spark.read
        .format("jdbc")
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
      spark.read
        .format("jdbc")
        .option("url", url)
        .option("dbtable", queryStr)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", 50000)
        .option("password", passwordFromConn(url, password))
        .load
    }
  }

  private def getMinMaxForColumn(spark: SparkSession,
                                 url: String,
                                 query: String,
                                 partitionColumn: String,
                                 password: String = ""): (Long, Long) = {
    val min_max_query =
      s"""(SELECT
         |coalesce(cast(min("$partitionColumn") as bigint), 0) as min,
         |coalesce(cast(max("$partitionColumn") as bigint),0) as max
         |FROM $query) AS tmp1""".stripMargin
    val row = spark.read
      .format("jdbc")
      .option("url", url)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", min_max_query)
      .option("password", passwordFromConn(url, password))
      .load
      .first
    val lowerBound = row.getLong(0)
    val upperBound = row.getLong(1)
    (lowerBound, upperBound)
  }

  def outputBulk(spark: SparkSession,
                 url: String,
                 table: String,
                 df: Dataset[Row],
                 path: String,
                 numPartitions: Int = 8,
                 password: String = "",
                 reindex: Boolean = false,
                 bulkLoadMode: BulkLoadMode = defaultBulkLoadStrategy,
                 bulkLoadBufferSize: Int = defaultBulkLoadBufferSize) = {
    bulkLoadMode match {
      case CSV =>
        outputBulkCsv(spark,
                      url,
                      table,
                      df,
                      path,
                      numPartitions,
                      password,
                      reindex,
                      bulkLoadBufferSize)
      case Stream =>
        outputCSVBulkStream(spark,
                            url,
                            table,
                            df,
                            numPartitions,
                            password,
                            reindex,
                            bulkLoadBufferSize)
      case PgBinaryStream =>
        outputBinaryBulkStream(spark,
                               url,
                               table,
                               df,
                               numPartitions,
                               password,
                               reindex,
                               bulkLoadBufferSize)
      case PgBinaryFiles =>
        outputBinaryBulkFiles(spark,
                              url,
                              table,
                              df,
                              path,
                              numPartitions,
                              password,
                              reindex,
                              bulkLoadBufferSize)
    }
  }

  def outputBulkCsv(spark: SparkSession,
                    url: String,
                    table: String,
                    df: Dataset[Row],
                    path: String,
                    numPartitions: Int = 8,
                    password: String = "",
                    reindex: Boolean = false,
                    bulkLoadBufferSize: Int = defaultBulkLoadBufferSize) = {
    logger.warn("using CSV strategy")
    try {
      if (reindex)
        indexDeactivate(url, table, password)
      val columns = df.schema.fields.map(x => s"${sanP(x.name)}").mkString(",")
      //transform arrays to string
      val dfTmp = dataframeToPgCsv(spark, df)
      //write a csv folder
      dfTmp.write
        .format("csv")
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

      outputBulkCsvLow(spark,
                       url,
                       table,
                       columns,
                       path,
                       numPartitions,
                       ",",
                       ".*.csv",
                       password,
                       bulkLoadBufferSize)
    } finally {
      if (reindex)
        indexReactivate(url, table, password)
    }
  }

  def outputBulkFileLow(spark: SparkSession,
                        url: String,
                        path: String,
                        sqlCopy: String,
                        extensionPattern: String,
                        numPartitions: Int = 8,
                        password: String = "",
                        bulkLoadBufferSize: Int = defaultBulkLoadBufferSize) = {

    // load the csv files from hdfs in parallel
    val fs = FileSystem.get(new Configuration())
    import spark.implicits._
    val rdd = fs
      .listStatus(new Path(path))
      .filter(x => x.getPath.toString.matches("^.*/" + extensionPattern + "$"))
      .map(x => x.getPath.toString)
      .toList
      .zipWithIndex
      .map(_.swap)
      .toDS
      .rdd
      .partitionBy(new ExactPartitioner(numPartitions))

    rdd.foreachPartition(x => {
      val conn = connOpen(url, password)
      x.foreach { s =>
        {
          val stream: InputStream = FileSystem
            .get(new Configuration())
            .open(new Path(s._2))
            .getWrappedStream
          val copyManager: CopyManager =
            new CopyManager(conn.asInstanceOf[BaseConnection])
          copyManager.copyIn(sqlCopy, stream, bulkLoadBufferSize)
        }
      }
      conn.close()
      x.toIterator
    })
  }

  def outputBulkCsvLow(
      spark: SparkSession,
      url: String,
      table: String,
      columns: String,
      path: String,
      numPartitions: Int = 8,
      delimiter: String = ",",
      extensionPattern: String = ".*.csv",
      password: String = "",
      bulkLoadBufferSize: Int = defaultBulkLoadBufferSize
  ) = {
    val csvSqlCopy =
      s"""COPY "$table" ($columns) FROM STDIN WITH CSV DELIMITER '$delimiter'  NULL '' ESCAPE '"' QUOTE '"' """
    outputBulkFileLow(spark,
                      url,
                      path,
                      csvSqlCopy,
                      extensionPattern,
                      numPartitions,
                      password,
                      bulkLoadBufferSize)
  }

  def outputCSVBulkStream(
      spark: SparkSession,
      url: String,
      table: String,
      df: Dataset[Row],
      numPartitions: Int = 8,
      password: String = "",
      reindex: Boolean = false,
      bulkLoadBufferSize: Int = defaultBulkLoadBufferSize,
      copyTimeoutMs: Long = defaultStreamBulkLoadTimeoutMs) = {
    logger.warn("using STREAM strategy")
    try {
      if (reindex)
        indexDeactivate(url, table, password)

      val delimiter = ","
      val schema = df.schema
      val columns = schema.fields.map(x => s"${sanP(x.name)}").mkString(",")
      //transform arrays to string
      val dfTmp = dataframeToPgCsv(spark, df)
      val dfTmpSchema = dfTmp.schema

      val rddToWrite: RDD[Row] =
        if (dfTmp.rdd.getNumPartitions > numPartitions)
          dfTmp.rdd.coalesce(numPartitions)
        else dfTmp.rdd

      rddToWrite.foreachPartition((p: Iterator[Row]) => {

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

        val csvOptions = new CSVOptions(csvOptionsMap,
                                        columnPruning = true,
                                        TimeZone.getDefault.getID)

        val outputWriter = new PipedWriter()
        val inputReader = new PipedReader(outputWriter, bulkLoadBufferSize)

        val univocityGenerator =
          new UnivocityGenerator(dfTmpSchema, outputWriter, csvOptions)
        val rowEncoder = RowEncoder.apply(dfTmpSchema)

        // Prepare a thread to copy data to PG asynchronously
        val promisedCopy = Promise[Unit]
        val sql =
          s"""COPY "$table" ($columns) FROM STDIN WITH CSV DELIMITER '$delimiter'  NULL '' ESCAPE '"' QUOTE '"' """
        val conn = connOpen(url, password)
        val copyManager: CopyManager =
          new CopyManager(conn.asInstanceOf[BaseConnection])
        val copyThread = new Thread("copy-to-pg-thread") {
          override def run(): Unit =
            promisedCopy.complete(
              Try(copyManager.copyIn(sql, inputReader, bulkLoadBufferSize)))
        }

        try {
          // Start the thread
          copyThread.start()
          try {
            // Load the copy stream reading from the partition
            var idx = 0L
            p.foreach(row => {
              idx += 1L
              // Check if the copying thread is still alive every thousand row to prevent the main
              // thread to produce unread data indefinitely
              if (idx % 1000 == 0 && promisedCopy.isCompleted) {
                // No need to check for None, promise has completed
                promisedCopy.future.value match {
                  case Some(Success(())) =>
                    throw new IllegalStateException(
                      "The copying thread finished successfully but not all data had been copied. This is very much unexpected!")
                  case Some(Failure(t)) =>
                    throw new IllegalStateException(
                      "The copying thread finished with an error.",
                      t)
                }
              }
              univocityGenerator.write(rowEncoder.toRow(row))
            })
            outputWriter.close()
            // Wait for the copy to have finished
            Await.result(promisedCopy.future,
                         Duration(copyTimeoutMs, TimeUnit.MILLISECONDS))
          } catch {
            case _: TimeoutException =>
              throw new TimeoutException(s"""
                   | The copy operation for copying this partition's data to postgres has been running for
                   | more than the timeout: ${TimeUnit.MILLISECONDS.toSeconds(
                                              copyTimeoutMs)}s.
                   | You can configure this timeout with option copyTimeoutMs, such as "2h", "100min",
                   | and default copyTimeout is "10min".
                """.stripMargin)
            // Rethrow the IllegalStateException caught in case of copy-thread error
            case ise: IllegalStateException => throw ise
          }
        } finally {
          // Finalize
          copyThread.interrupt()
          copyThread.join()
          inputReader.close()
          conn.close()
        }
      })

    } finally {
      if (reindex)
        indexReactivate(url, table, password)
    }
  }

  def outputBinaryBulkStream(
      spark: SparkSession,
      url: String,
      table: String,
      df: Dataset[Row],
      numPartitions: Int = 8,
      password: String = "",
      reindex: Boolean = false,
      bulkLoadBufferSize: Int = defaultBulkLoadBufferSize
  ) = {
    logger.warn("using PG Binary stream strategy")
    try {
      if (reindex)
        indexDeactivate(url, table, password)

      val schema = df.schema
      val columns = schema.fields.map(x => s"${sanP(x.name)}")

      // First convert spark Rows to binary
      val rddToStream: RDD[Array[Byte]] =
        dataframeMapsAndStructsToJson(spark, df).rdd
          .mapPartitions((p: Iterator[Row]) => {
            val rowWriter = PgBulkInsertConverter.makeRowWriter(schema)

            val pgBinaryConverter = new PgBinaryConverter(rowWriter)

            val it = p.map(sparkRow => {
              pgBinaryConverter.convertRow(sparkRow)
            })
            it
          })

      // Then write binary after coalescing
      val rddToStreamCoalesced =
        if (rddToStream.getNumPartitions > numPartitions)
          rddToStream.coalesce(numPartitions)
        else rddToStream

      rddToStreamCoalesced.foreachPartition((p: Iterator[Array[Byte]]) => {
        val conn = PostgreSqlUtils.getPGConnection(connOpen(url, password))

        val fullyQualifiedTableName =
          PostgreSqlUtils.getFullyQualifiedTableName(null, table, true)
        val commaSeparatedColumns =
          columns.map(c => PostgreSqlUtils.quoteIdentifier(c)).mkString(", ")
        val copyCommand =
          s"COPY $fullyQualifiedTableName($commaSeparatedColumns) FROM STDIN BINARY"

        val pgCopyOutputStream = new DataOutputStream(
          new PGCopyOutputStream(conn, copyCommand, bulkLoadBufferSize))

        // PG binary header
        pgCopyOutputStream.write(
          "PGCOPY\nÿ\r\n\u0000".getBytes(Charsets.ISO_8859_1))
        pgCopyOutputStream.writeInt(0)
        pgCopyOutputStream.writeInt(0)

        p.foreach(binaryRow => {
          pgCopyOutputStream.write(binaryRow)
        })

        pgCopyOutputStream.writeShort(-1)
        pgCopyOutputStream.close()
      })

    } finally {
      if (reindex)
        indexReactivate(url, table, password)
    }
  }

  def outputBinaryBulkFiles(
      spark: SparkSession,
      url: String,
      table: String,
      df: Dataset[Row],
      path: String,
      numPartitions: Int = 8,
      password: String = "",
      reindex: Boolean = false,
      bulkLoadBufferSize: Int = defaultBulkLoadBufferSize
  ) = {
    logger.warn("using PG-Binary files strategy")
    try {
      if (reindex)
        indexDeactivate(url, table, password)

      val schema = df.schema
      val columns = schema.fields.map(x => s"${sanP(x.name)}")

      //  Write spark Rows to binary
      dataframeMapsAndStructsToJson(spark, df).rdd
        .mapPartitionsWithIndex((partIdx: Int, p: Iterator[Row]) => {
          val filePath = new Path(f"$path%s/part-$partIdx%05d.pgb")
          val fs = FileSystem.get(new Configuration())

          val outputStream = new DataOutputStream(fs.create(filePath))

          val rowWriter = PgBulkInsertConverter.makeRowWriter(schema)
          val pgBinaryWriter = new PgBinaryWriter(outputStream, rowWriter)

          // PG binary header
          outputStream.write(
            "PGCOPY\nÿ\r\n\u0000".getBytes(Charsets.ISO_8859_1))
          outputStream.writeInt(0)
          outputStream.writeInt(0)

          p.foreach(sparkRow => {
            pgBinaryWriter.writeRow(sparkRow)
          })

          outputStream.writeShort(-1)
          outputStream.close()

          // No foreach with partition-index, faking returning data and force execution with take(1)
          Seq.empty[Row].toIterator
        })
        .take(1)

      // Then write binary after coalescing
      val fullyQualifiedTableName =
        PostgreSqlUtils.getFullyQualifiedTableName(null, table, true)
      val commaSeparatedColumns =
        columns.map(c => PostgreSqlUtils.quoteIdentifier(c)).mkString(", ")
      val copyCommand =
        s"COPY $fullyQualifiedTableName($commaSeparatedColumns) FROM STDIN BINARY"

      outputBulkFileLow(spark,
                        url,
                        path,
                        copyCommand,
                        ".*.pgb",
                        numPartitions,
                        password,
                        bulkLoadBufferSize)

    } finally {
      if (reindex)
        indexReactivate(url, table, password)
    }
  }

  def output(url: String,
             table: String,
             df: Dataset[Row],
             batchsize: Int = 50000,
             password: String = "") = {
    df.coalesce(8)
      .write
      .mode(org.apache.spark.sql.SaveMode.Overwrite)
      .format("jdbc")
      .option("url", url)
      .option("dbtable", table)
      .option("batchsize", batchsize)
      .option("password", passwordFromConn(url, password))
      .option("driver", "org.postgresql.Driver")
      .save()
  }

  def inputQueryPartBulkCsv(spark: SparkSession,
                            fsConf: String,
                            url: String,
                            query: String,
                            path: String,
                            numPartitions: Int,
                            partitionColumn: String,
                            splitFactor: Int = 1,
                            password: String = "") = {
    val columnType =
      getSqlColumnType(spark, url, query, partitionColumn, password)
    val queryStr = s"($query) as tmp"
    val rdd =
      if ("Long" :: "Integer" :: Nil contains columnType) {
        val (lowerBound, upperBound) =
          getMinMaxForColumn(spark, url, queryStr, partitionColumn)
        getPartitions(spark, lowerBound, upperBound, numPartitions, splitFactor)
      } else {
        val (lowerBound, upperBound) =
          getMinMaxForColumnString(spark, url, queryStr, partitionColumn)
        getPartitionsString(spark,
                            lowerBound,
                            upperBound,
                            numPartitions,
                            splitFactor)
      }

    rdd.foreachPartition(rddPart => {
      val conn = connOpen(url, password)
      rddPart.foreach { s =>
        {
          val queryPart =
            s"""SELECT * FROM $queryStr WHERE "$partitionColumn" ${s._2}"""
          inputQueryBulkCsv(fsConf, conn, queryPart, path)
        }
      }
      conn.close()
      rddPart.toIterator
    })
  }

  def getSqlColumnType(spark: SparkSession,
                       url: String,
                       query: String,
                       column: String,
                       password: String = "") = {
    val stmt = s"""select "${column}" from (${query}) t limit 0"""
    val res = sqlExecWithResult(spark, url, stmt, password)
    val columnType: Array[String] =
      res.schema.fields.map(f => f.dataType.typeName)
    columnType(0) match {
      case "string"  => "String"
      case "long"    => "Long"
      case "integer" => "Integer"
      case e =>
        throw new UnsupportedOperationException(
          s"${e} not supported for partition column")
    }
  }

  def inputQueryBulkCsv(fsConf: String,
                        conn: Connection,
                        query: String,
                        path: String) = {
    val sqlStr =
      s""" COPY ($query) TO STDOUT  WITH DELIMITER AS ',' CSV NULL '' ENCODING 'UTF-8' QUOTE '"' ESCAPE '"' """
    val copyInputStream: PGCopyInputStream =
      new PGCopyInputStream(conn.asInstanceOf[BaseConnection], sqlStr)

    val conf = new Configuration()
    conf.set("fs.defaultFS", fsConf)
    val fs = FileSystem.get(conf)
    val output =
      fs.create(new Path(path, "part-" + randomUUID.toString + ".csv"))

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

    sqlExec(url, query2, password)
  }

  def inputQueryBulkDf(spark: SparkSession,
                       url: String,
                       query: String,
                       path: String,
                       isMultiline: Boolean = false,
                       numPartitions: Int = 1,
                       partitionColumn: String = "",
                       splitFactor: Int = 1,
                       password: String = ""): Dataset[Row] = {
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
      inputQueryPartBulkCsv(spark,
                            fsConf,
                            url,
                            query,
                            path,
                            numPartitions,
                            partitionColumn,
                            splitFactor,
                            password)
    }

    val schemaQuerySimple = schemaSimplify(schemaQueryComplex)
    // read the resulting csv
    val dfSimple = spark.read
      .format("csv")
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

  def dataframeFromPgCsv(spark: SparkSession,
                         dfSimple: Dataset[Row],
                         schemaQueryComplex: StructType): Dataset[Row] = {
    val tableTmp = "table_" + randomUUID.toString.replaceAll(".*-", "")
    dfSimple.createOrReplaceTempView(tableTmp)
    val sqlQuery = "SELECT " + schemaQueryComplex
      .map(a => {
        if (a.dataType.simpleString == "boolean") {
          "CAST(" + sanS(a.name) + " as boolean) as " + sanS(a.name)
        } else if (a.dataType.simpleString.indexOf("array") == 0) {
          "CAST(SPLIT(REGEXP_REPLACE(" + sanS(a.name) + ", '^[{]|[}]$', ''), ',') AS " + a.dataType.simpleString + ") as " + sanS(
            a.name)
        } else {
          sanS(a.name)
        }
      })
      .mkString(", ") + " FROM " + sanS(tableTmp)
    spark.sql(sqlQuery)
  }

  def dataframeToPgCsv(spark: SparkSession, df: Dataset[Row]): DataFrame = {
    val newCols = df.schema.fields.map(c => {
      val colName = c.name
      if (c.dataType.simpleString.indexOf("array") == 0) {
        regexp_replace(
          regexp_replace(col(colName).cast("string"), lit("^."), lit("{")),
          lit(".$"),
          lit("}")).as(colName)
      } else if (c.dataType.simpleString.indexOf("string") == 0) {
        regexp_replace(regexp_replace(col(colName), lit("\\u0000"), lit("")),
                       lit("\r\n|\r"),
                       lit("\n")).as(colName)
      } else {
        col(colName)
      }
    })
    df.select(newCols: _*)
  }

  def dataframeMapsAndStructsToJson(spark: SparkSession,
                                    df: Dataset[Row]): DataFrame = {
    val newCols = df.schema.fields.map(c => {
      val colName = c.name
      if (c.dataType.isInstanceOf[MapType] || c.dataType
            .isInstanceOf[StructType]) {
        to_json(col(colName)).as(colName)
      } else {
        col(colName)
      }
    })
    df.select(newCols: _*)
  }

  def tableEmpty(spark: SparkSession,
                 url: String,
                 table: String,
                 password: String): Boolean = {
    val query = s"""select 1 from "$table" limit 1 """
    sqlExecWithResult(spark, url, query, password).count == 0
  }

  def loadEmptyTable(spark: SparkSession,
                     url: String,
                     table: String,
                     candidate: Dataset[Row],
                     path: String,
                     partitions: Int,
                     password: String,
                     bulkLoadMode: BulkLoadMode): Boolean = {
    if (tableEmpty(spark, url, table, password)) {
      logger.warn("Loading directly data")
      outputBulk(spark,
                 url,
                 table,
                 candidate,
                 path,
                 partitions,
                 password,
                 reindex = false,
                 bulkLoadMode)
      return true
    }
    false
  }

  def outputBulkDfScd1Hash(
      spark: SparkSession,
      url: String,
      table: String,
      candidate: Dataset[Row],
      key: Seq[String],
      partitions: Int = 4,
      path: String,
      scdFilter: Option[String] = None,
      deleteSet: Option[String] = None,
      password: String = "",
      bulkLoadMode: BulkLoadMode = defaultBulkLoadStrategy,
      bulkLoadBufferSize: Int = defaultBulkLoadBufferSize): Unit = {
    if (loadEmptyTable(spark,
                       url,
                       table,
                       candidate,
                       path,
                       partitions,
                       password,
                       bulkLoadMode))
      return

    logger.warn("The postgres table is not empty")

    val insertTmp = getTmpTable("ins_")
    val updateTmp = getTmpTable("upd_")
    val deleteTmp = getTmpTable("del_")
    val isDelete = deleteSet.isDefined

    try {
      // 1. get key/hash
      val queryFetch1 = """select  %s, "%s" from %s where %s""".format(
        key.mkString("\"", "\",\"", "\""),
        "hash",
        sanP(table),
        scdFilter.getOrElse(" TRUE"))
      val fetch1 = inputQueryBulkDf(spark,
                                    url,
                                    queryFetch1,
                                    path,
                                    isMultiline = true,
                                    partitions,
                                    key(0),
                                    40,
                                    password)

      // 2.1 produce insert
      val joinCol = key.map(x => s"""f.`$x` = c.`$x`""").mkString(" AND ")
      val insert =
        candidate.as("c").join(fetch1.as("f"), expr(joinCol), "left_anti")
      logger.info(s"Row to insert: ${insert.count}")

      // 2.2 produce insert
      val update = candidate
        .as("c")
        .join(fetch1.as("f"),
              expr(joinCol + "AND c.hash != f.hash"),
              "left_semi")
      logger.info(s"Row to update: ${update.count}")

      // 3. load tmp tables
      tableCreate(url, insertTmp, insert.schema, password)

      outputBulk(spark,
                 url,
                 insertTmp,
                 insert,
                 path + "ins",
                 partitions,
                 password,
                 bulkLoadMode = bulkLoadMode,
                 bulkLoadBufferSize = bulkLoadBufferSize)
      insert.unpersist()

      tableCreate(url, updateTmp, update.schema, password)
      outputBulk(spark,
                 url,
                 updateTmp,
                 update,
                 path + "upd",
                 partitions,
                 password,
                 bulkLoadMode = bulkLoadMode,
                 bulkLoadBufferSize = bulkLoadBufferSize)
      update.unpersist()

      // 4. load postgres
      sqlExec(url,
              applyScd1(table, insertTmp, updateTmp, insert.schema, key),
              password)

      // 5 produce delete
      if (isDelete) {
        val delete = fetch1
          .as("f")
          .join(candidate.as("c"), expr(joinCol), "left_anti")
          .selectExpr(key.mkString("`", "`,`", "`").split(","): _*)
        tableCreate(url, deleteTmp, delete.schema, password)
        outputBulk(spark,
                   url,
                   deleteTmp,
                   delete,
                   path + "del",
                   partitions,
                   password,
                   bulkLoadMode = bulkLoadMode,
                   bulkLoadBufferSize = bulkLoadBufferSize)
        sqlExec(url,
                applyScd1Delete(table, deleteTmp, key, deleteSet.get),
                password)
      }

    } finally {
      // 5. drop the temporarytables
      tableDrop(url, insertTmp, password)
      tableDrop(url, updateTmp, password)
      if (isDelete)
        tableDrop(url, deleteTmp, password)
    }
  }

  def applyScd1Delete(table: String,
                      deleteTmp: String,
                      key: Seq[String],
                      deleteSet: String): String = {
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

  def applyScd1(table: String,
                insertTmp: String,
                updateTmp: String,
                insertSchema: StructType,
                key: Seq[String]): String = {
    val insertCol =
      insertSchema.fields.map(f => f.name).mkString("\"", "\",\"", "\"")
    val updateCol = insertSchema.fields
      .map(f => s""" "${f.name}" = s."${f.name}" """)
      .mkString(",")
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
    logger.info(query)
    query
  }

  def getTmpTable(str: String): String = {
    val res = str + randomUUID.toString.replaceAll(".*-", "")
    res
  }

  def applyScd2(table: String,
                insertTmp: String,
                updateTmp: String,
                pk: String,
                endDatetimeCol: String,
                insertSchema: StructType): String = {
    val insertCol =
      insertSchema.fields.map(f => f.name).mkString("\"", "\",\"", "\"")
    val query =
      s"""
         |WITH upd as (
         |  UPDATE "$table" set "$endDatetimeCol" = now() WHERE "$pk" IN (SELECT "$pk" FROM "$updateTmp")
         |)
         |INSERT INTO "$table" ($insertCol, "$endDatetimeCol")
         |SELECT $insertCol, null as "$endDatetimeCol"
         |FROM "$insertTmp"
         |""".stripMargin
    logger.info(query)
    query
  }

  def outputBulkDfScd2Hash(
      spark: SparkSession,
      url: String,
      table: String,
      candidate: DataFrame,
      pk: String,
      key: Seq[String],
      endDatetimeCol: String,
      partitions: Int,
      path: String,
      password: String = "",
      bulkLoadMode: BulkLoadMode = defaultBulkLoadStrategy,
      bulkLoadBufferSize: Int = defaultBulkLoadBufferSize): Unit = {

    if (loadEmptyTable(spark,
                       url,
                       table,
                       candidate,
                       path,
                       partitions,
                       password,
                       bulkLoadMode))
      return

    val insertTmp = getTmpTable("ins_")
    val updateTmp = getTmpTable("upd_")
    try {
      // 1. get the pk/key/hash
      val queryFetch1 =
        """select "%s", %s, "%s" from %s where "%s" is null """.format(
          pk,
          key.mkString("\"", "\",\"", "\""),
          "hash",
          table,
          endDatetimeCol)
      val fetch1 = inputQueryBulkDf(spark,
                                    url,
                                    queryFetch1,
                                    path,
                                    isMultiline = true,
                                    partitions,
                                    pk,
                                    1,
                                    password)

      // 2.1 produce insert and update
      val joinCol = key.map(x => s"""f.`$x` = c.`$x`""").mkString(" AND ")
      val insert = candidate
        .as("c")
        .join(fetch1.as("f"),
              expr(joinCol + "AND c.hash = f.hash"),
              "left_anti")

      // 2.2 produce insert and update
      val update = fetch1
        .as("f")
        .join(candidate.as("c"),
              expr(joinCol + "AND c.hash != f.hash"),
              "left_semi")
        .select(col(pk))

      // 3. load tmp tables
      tableCreate(url, insertTmp, insert.schema, password)
      outputBulk(spark,
                 url,
                 insertTmp,
                 insert,
                 path + "ins",
                 partitions,
                 password,
                 bulkLoadMode = bulkLoadMode,
                 bulkLoadBufferSize = bulkLoadBufferSize)

      tableCreate(url, updateTmp, update.schema, password)
      outputBulk(spark,
                 url,
                 updateTmp,
                 update,
                 path + "upd",
                 partitions,
                 password,
                 bulkLoadMode = bulkLoadMode,
                 bulkLoadBufferSize = bulkLoadBufferSize)

      // 4. load postgres
      sqlExec(url,
              applyScd2(table,
                        insertTmp,
                        updateTmp,
                        pk,
                        endDatetimeCol,
                        insert.schema),
              password)

    } finally {
      // 5. drop the temporarytables
      tableDrop(url, insertTmp, password)
      tableDrop(url, updateTmp, password)
    }
  }

  def sqlExecWithResult(spark: SparkSession,
                        url: String,
                        query: String,
                        password: String = "",
                        params: List[Any] = Nil): Dataset[Row] = {
    val conn = connOpen(url, password)
    try {

      val st: PreparedStatement = conn.prepareStatement(query)
      val rs = parametrize(st, params).executeQuery()

      import scala.collection.mutable.ListBuffer
      var c = new ListBuffer[Row]()
      while (rs.next()) {
        val b = (1 to rs.getMetaData.getColumnCount).map { idx =>
          {
            val res = rs.getMetaData.getColumnClassName(idx) match {
              case "java.lang.String"     => rs.getString(idx)
              case "java.lang.Boolean"    => rs.getBoolean(idx)
              case "java.lang.Long"       => rs.getLong(idx)
              case "java.lang.Integer"    => rs.getInt(idx)
              case "java.math.BigDecimal" => rs.getDouble(idx)
              case "java.sql.Date"        => rs.getDate(idx)
              case "java.sql.Timestamp"   => rs.getTimestamp(idx)
              case _                      => rs.getString(idx)
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

  def parametrize(st: PreparedStatement, params: List[Any]) = {
    for ((obj, i) <- params.zipWithIndex) {
      obj match {
        case s: String               => st.setString(i + 1, s)
        case b: Boolean              => st.setBoolean(i + 1, b)
        case l: Long                 => st.setLong(i + 1, l)
        case i: Integer              => st.setInt(i + 1, i)
        case b: java.math.BigDecimal => st.setDouble(i + 1, b.doubleValue())
        case d: java.sql.Date        => st.setDate(i + 1, d)
        case t: Timestamp            => st.setTimestamp(i + 1, t)
        case _ =>
          throw new UnsupportedEncodingException(
            obj.getClass.getCanonicalName + " type not yet supported for prepared statements")
      }
    }
    st
  }

  def jdbcMetadataToStructType(meta: ResultSetMetaData): StructType = {
    StructType((1 to meta.getColumnCount).map { idx =>
      meta.getColumnClassName(idx) match {
        case "java.lang.String" =>
          StructField(meta.getColumnLabel(idx), StringType)
        case "java.lang.Boolean" =>
          StructField(meta.getColumnLabel(idx), BooleanType)
        case "java.lang.Integer" =>
          StructField(meta.getColumnLabel(idx), IntegerType)
        case "java.lang.Long" => StructField(meta.getColumnLabel(idx), LongType)
        case "java.math.BigDecimal" =>
          StructField(meta.getColumnLabel(idx), DoubleType)
        case "java.sql.Date" => StructField(meta.getColumnLabel(idx), DateType)
        case "java.sql.Timestamp" =>
          StructField(meta.getColumnLabel(idx), TimestampType)
        case _ => StructField(meta.getColumnLabel(idx), StringType)
      }
    })
  }

  private def getMinMaxForColumnString(
      spark: SparkSession,
      url: String,
      query: String,
      partitionColumn: String,
      password: String = ""): (String, String) = {
    val min_max_query =
      s"""SELECT
         |min("$partitionColumn") as min,
         |max("$partitionColumn") as max
         |FROM $query""".stripMargin
    val row = sqlExecWithResult(spark, url, min_max_query, password).first
    val lowerBound = row.getString(0)
    val upperBound = row.getString(1)
    (lowerBound, upperBound)
  }

  private def getSchemaQuery(spark: SparkSession,
                             url: String,
                             query: String,
                             password: String = ""): StructType = {
    val queryStr = s"""(SELECT * FROM ($query) as tmp1 LIMIT 0) as tmp"""
    spark.read
      .format("jdbc")
      .option("url", url)
      .option("password", passwordFromConn(url, password))
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", queryStr)
      .load
      .schema
  }

  private def getPartitions(spark: SparkSession,
                            lowerBound: Long,
                            upperBound: Long,
                            numPartitions: Int,
                            splitFactor: Int = 1): RDD[(Int, String)] = {
    val length = BigInt(1) + upperBound - lowerBound
    val splitPartitions = numPartitions * splitFactor

    import spark.implicits._
    val partitions = (0 until splitPartitions)
      .map { i =>
        val start = lowerBound + (i * length / splitPartitions)
        val end = lowerBound + ((i + 1) * length / splitPartitions) - 1
        (start, end)
      }
      .filter { case (x, y) => x <= y } // remove the cases x > y
      .map { case (start, end) => f"between $start AND $end" }
      .zipWithIndex
      .map(_.swap)
      .toDS
      .dropDuplicates("_2") // remove the duplicated predicate, in case the splitfactor is too high
      .rdd
      .partitionBy(new ExactPartitioner(numPartitions))
    partitions
  }

  /**
    * The below implementation wont work for utf8 characters. It only suppoert ascii charsets
    * It will split numbers better than alpha chars.
    * It is possible to extend this behavior by adding several elements in the ascii array
    * @see https://www.cs.cmu.edu/~pattis/15-1XX/common/handouts/ascii.html
    * @param spark
    * @param lowerString
    * @param upperString
    * @param numPartitions
    * @param splitFactor
    * @return
    */
  private def getPartitionsString(spark: SparkSession,
                                  lowerString: String,
                                  upperString: String,
                                  numPartitions: Int,
                                  splitFactor: Int = 1): RDD[(Int, String)] = {

    logger.debug(s"low:${lowerString} up:${upperString}")
    import spark.implicits._
    if (lowerString == upperString)
      return ((1, s"between '${lowerString}' and '${upperString}'") :: Nil).toDS.rdd

    val commonStart = (lowerString, upperString).zipped.toIterator
      .takeWhile { case (l: Char, u: Char) => l == u }
      .map(x => x._1)
      .toList
      .mkString("")

    logger.debug(s"common: ${commonStart}")
    val lowerBound =
      lowerString.substring(commonStart.length, lowerString.length)
    val upperBound =
      upperString.substring(commonStart.length, upperString.length)

    import spark.implicits._
    val partitionsTmp = List((lowerBound, "/"),
                             ("0", "0"),
                             ("1", "1"),
                             ("2", "2"),
                             ("3", "3"),
                             ("4", "4"), //
                             ("5", "5"),
                             ("6", "6"),
                             ("7", "7"),
                             ("8", "8"),
                             ("9", "9"),
                             (":", upperBound))
      .filterNot { case (_, end) => end > upperBound }

    val partitionsLast = List(partitionsTmp.last._1 -> upperBound)

    val partitions =
      (partitionsTmp.reverse.drop(1) ++ partitionsLast)
        .map {
          case (start: String, end: String) =>
            val startString = commonStart + start
            val endString = commonStart + end
            f"between '$startString' AND '$endString'"
        }
        .zipWithIndex
        .map(_.swap)
        .toDS
        .rdd
        .partitionBy(new ExactPartitioner(numPartitions))
    logger.debug(partitions.take(1000).mkString("\n"))
    partitions
  }

}

class ExactPartitioner(partitions: Int) extends Partitioner {

  def getPartition(key: Any): Int = {
    key match {
      case l: Long => math.abs(l.toInt % numPartitions())
      case i: Int  => math.abs(i % numPartitions())
      case _       => math.abs(key.hashCode() % numPartitions())
    }
  }

  def numPartitions(): Int = partitions
}
