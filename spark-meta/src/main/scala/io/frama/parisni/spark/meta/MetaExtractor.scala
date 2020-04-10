package io.frama.parisni.spark.meta

import com.typesafe.scalalogging.LazyLogging
import io.frama.parisni.spark.dataframe.DFTool
import io.frama.parisni.spark.meta.extractor.FeatureExtractTrait
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class MetaExtractor(extractor: FeatureExtractTrait, spark: SparkSession, host: String, database: String, user: String, dbType: String, schema: String)
  extends GetTables with LazyLogging {

  /**
   * Alternative constructor
   */
  def this(confSchema: ConfigMetaYaml.Schema, spark: SparkSession, schema: String = "public") {

    this(io.frama.parisni.spark.meta.extractor.Utils.getFeatureExtractImplClass(confSchema.extractor)
      , spark, confSchema.host, confSchema.db, confSchema.user, confSchema.dbType, schema)
  }

  def fetchTable(sql: String): DataFrame = {
    logger.warn(s"$host, $database, $schema, $sql")
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

  def initTables(dbName: String): Unit = {
    val result = dbType match {
      case "postgresql" => getPostgresTable(dbName)
      case "spark" => getSparkTable(dbName)
    }

    var res = extractor.extractSource(result)

    // extraire la pk
    res = extractor.extractPrimaryKey(res)

    // extraire les fk
    res = extractor.extractForeignKey(res).cache


    resdatabase = extractor.generateDatabase(res)
    resschema = extractor.generateSchema(res)
    restable = extractor.generateTable(res)
    rescolumn = extractor.generateColumn(res)
    resreference = extractor.inferForeignKey(res)
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
    val regularTable = DFTool.trimAll(fetchTable(SQL_HIVE_TABLE.format(dbName)))
    val externalTable = extractJson(DFTool.trimAll(fetchTable(SQL_HIVE_TABLE_EXT.format(dbName))))
    DFTool.unionDataFrame(regularTable, externalTable)
  }

  protected def getPostgresTable(dbName: String): DataFrame = {
    val tbl = fetchTable(SQL_PG_TABLE.format(dbName))
    val view = fetchTable(SQL_PG_VIEW.format(dbName))
    val res = DFTool.trimAll(DFTool.unionDataFrame(tbl, view))
    res
  }

}