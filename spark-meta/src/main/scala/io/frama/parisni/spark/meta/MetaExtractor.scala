package io.frama.parisni.spark.meta


import java.sql.{Connection, ResultSet}

import com.typesafe.scalalogging.LazyLogging
import io.frama.parisni.spark.dataframe.DFTool
import io.frama.parisni.spark.meta.strategy.{MetaStrategy, MetaStrategyBuilder}
import io.frama.parisni.spark.postgres.PGTool
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class MetaExtractor(metaStrategy: MetaStrategy, spark: SparkSession
                    , host: String, database: String, user: String, dbType: String, schema: String)
  extends GetTables with LazyLogging {

  /**
   * Alternative constructor
   */
  def this(confSchema: ConfigMetaYaml.Schema, spark: SparkSession, schema: String = "public") {

    this(MetaStrategyBuilder.build(confSchema.strategy), spark
      , confSchema.host, confSchema.db, confSchema.user, confSchema.dbType, schema)
  }

  def getUrl(): String = {
    //TODO parameterized port value to remove hardcoded 5432
    f"jdbc:postgresql://${host}:5432/${database}?user=${user}&currentSchema=${schema}"
  }

  def fetchTable(sql: String): DataFrame = {
    logger.info(s"$host, $database, $schema, $sql")
    spark.read.format("postgres")
      .option("query", sql)
      .option("host", host)
      .option("user", user)
      .option("database", database)
      .option("schema", schema)
      .option("partitions", 1)
      .option("multiline", value = true)
      .load
  }

  def initTables(dbName: String, schemaRegexFilter: Option[String]): Unit = {
    val result = dbType match {
      case "postgresql" => getPostgresTable(dbName)
      case "spark" => getSparkTable(dbName)
    }
    var res = metaStrategy.extractor.extractSource(result)
      .filter(col("lib_schema").rlike(schemaRegexFilter.getOrElse(".*")))

    // extraire la pk
    res = metaStrategy.extractor.extractPrimaryKey(res)

    // extraire les fk
    res = metaStrategy.extractor.extractForeignKey(res).cache

    resdatabase = metaStrategy.generator.generateDatabase(res)
    resschema = metaStrategy.generator.generateSchema(res)
    restable = metaStrategy.generator.generateTable(res)
    rescolumn = metaStrategy.generator.generateColumn(res)
    resreference = metaStrategy.extractor.inferForeignKey(res)
  }

  var resdatabase: DataFrame = _
  var resschema: DataFrame = _
  var restable: DataFrame = _
  var rescolumn: DataFrame = _
  var resreference: DataFrame = _

  def getDatabase: DataFrame = this.resdatabase

  def getSchema: DataFrame = this.resschema

  def getTable: DataFrame = this.restable

  def getColumn: DataFrame = this.rescolumn

  def getReference: DataFrame = this.resreference

  def extractJson(df: DataFrame): DataFrame = {
    val stats = df.selectExpr("lib_database", "lib_schema", "lib_table", "explode(stts) as e")
      .withColumn("param", expr("split(e,'=')[0]"))
      .withColumn("value", expr("split(e,'=')[1]"))
      .withColumn("lib_column", expr("regexp_extract(param,'.*?([^\\.]+)\\.[^\\.]+$', 1)"))
      .withColumn("type_value", expr("regexp_extract(param,'.*?[^\\.]+\\.([^\\.]+)$', 1)"))


    val res = df.withColumn("js", expr("from_json(schem, 'struct<fields:array<struct<metadata:struct<>,name:string,nullable:boolean,type:string>>,type:string>')"))
      .selectExpr("posexplode(js.fields)", "lib_database", "lib_schema", "lib_table", "count_table", "last_analyze")
      .selectExpr("lib_database", "lib_schema", "lib_table",
        "count_table", "last_analyze",
        "col.name as lib_column", "col.type as typ_column",
        "col.nullable as is_mandatory", "pos + 1 as order_column")
      .join(stats.filter(expr("type_value = 'nullCount'")).as("nc"), Seq("lib_database", "lib_schema", "lib_table", "lib_column"), "left")
      .join(stats.filter(expr("type_value = 'distinctCount'")).as("dc"), Seq("lib_database", "lib_schema", "lib_table", "lib_column"), "left")
      .selectExpr("lib_database", "lib_schema", "lib_table",
        "count_table", "last_analyze",
        "cast(dc.value as bigint) count_distinct_column", "100 * cast(nc.value as bigint) / count_table null_ratio_column",
        "lib_column", "typ_column",
        "is_mandatory", "order_column")
    res.show(false)
    res
  }

  protected def getSparkTable(dbName: String): DataFrame = {
    val regularTable = DFTool.trimAll(fetchTable(GetTables.SQL_HIVE_TABLE.format(dbName)))
    val externalTable = extractJson(DFTool.trimAll(fetchTable(GetTables.SQL_HIVE_TABLE_EXT.format(dbName))))
    DFTool.unionDataFrame(regularTable, externalTable)
      .withColumn("last_commit_timestampz", expr("cast(null as timestamp)"))
  }

  protected def getPostgresTable(dbName: String): DataFrame = {

    val tbl = addLastCommitTimestampColumn(fetchTable(GetTables.SQL_PG_TABLE.format(dbName)), "last_commit_timestampz")
    val view = fetchTable(GetTables.SQL_PG_VIEW.format(dbName))
    val res = DFTool.trimAll(DFTool.unionDataFrame(tbl, view))

    res
  }

  /**
   * Reads the postgres configuration status of parameter "track_commit_timestamp".
   * This parameter needs to be set to "on" to be able to extract the last commit timestamp.
   *
   * @return 'true' if "track_commit_timestamp" is on, 'false' otherwise
   */
  def is_track_commit_timestamp_activate(): Boolean = {
    val conn: Connection = PGTool.connOpen(getUrl())
    val resultSet: ResultSet = conn.createStatement().executeQuery("show track_commit_timestamp")
    resultSet.next()
    val res: Boolean = resultSet.getString("track_commit_timestamp").equals("on")
    conn.close()
    res
  }

  /**
   * Returns new dataset by adding new column 'last_commit_timestampz' of type 'timestamp' as the per table last commit timestamp.
   *
   * @param dataFrame Input dataframe. It should contain at least theses three columns : "lib_database", "lib_schema" and "lib_table"
   * @param colName   Name to give to the added column
   * @return returns a new dataframe with the newColum if 'track_commit_timestamp' is activated ,
   *         otherwise it returns the input dataframe without any transformation
   */
  def addLastCommitTimestampColumn(dataFrame: DataFrame, colName: String): DataFrame = {

    //first of all we check if track_commit_timestamp is activated
    if (!is_track_commit_timestamp_activate()) {
      logger.warn("postgres parameter : track_commit_timestamp = off")
      return dataFrame.withColumn(colName, expr("cast(null as timestamp)"))
    }
    logger.info("postgres parameter : track_commit_timestamp = on")

    val timestamp_df: DataFrame = PGTool.sqlExecWithResult(spark, getUrl(), buildLastCommitTimestampQuery(dataFrame, colName))

    //Add new column with last_commit_timestamp
    dataFrame.join(timestamp_df, Seq("lib_database", "lib_schema", "lib_table"), "left_outer")
  }

  private def buildLastCommitTimestampQuery(df: DataFrame, colName: String): String = {
    df.dropDuplicates("lib_database", "lib_schema", "lib_table")
      .select("lib_database", "lib_schema", "lib_table")
      .collect()
      .map(row =>
        s"""(SELECT * FROM (
           |  SELECT pg_xact_commit_timestamp(xmin)::timestamptz as $colName,
           |  '${row.getString(0)}'::text as lib_database,
           |  '${row.getString(1)}'::text as lib_schema,
           |  '${row.getString(2)}'::text as lib_table
           |  from ${row.getString(1)}.${row.getString(2)}) tmp_table
           |WHERE tmp_table.$colName is not null
           |order by tmp_table.$colName desc limit 1)""".stripMargin)
      .mkString(" UNION ALL ")
  }
}
