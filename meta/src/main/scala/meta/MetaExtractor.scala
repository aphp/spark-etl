package meta

import com.typesafe.scalalogging.LazyLogging
import io.frama.parisni.spark.dataframe.DFTool
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class MetaExtractor(spark: SparkSession, host: String, database: String, user: String, dbType: String, schema: String = "public") extends FeatureExtract with GetTables with LazyLogging {

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

    var res = extractSource(result)

    // extraire la pk
    res = extractPrimaryKey(res)

    // extraire les fk
    res = extractForeignKey(res).cache


    resdatabase = generateDatabase(res)
    resschema = generateSchema(res)
    restable = generateTable(res)
    rescolumn = generateColumn(res)
    resreference = inferForeignKey(res)
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


trait GetTables {

  val SQL_HIVE_TABLE: String =
    """
       select
             '%s' lib_database,
             d."NAME" lib_schema,
             t."TBL_NAME" lib_table,
             "TBL_TYPE" typ_table,
             numrow."PARAM_VALUE"::bigint count_table,
             case when numrow."PARAM_VALUE" is null then null::timestamp else to_timestamp(cast(dt."PARAM_VALUE" as bigint))::timestamp end  last_analyze,
             "COLUMN_NAME" lib_column,
             "TYPE_NAME" typ_column,
             "INTEGER_IDX" + 1 order_column,
             dist."PARAM_VALUE"::bigint count_distinct_column,
             (100 * nc."PARAM_VALUE"::bigint / numrow."PARAM_VALUE"::bigint)::float null_ratio_column,
             "COMMENT" comment_fonctionnel,
              cast(null as boolean) as is_mandatory,
              null::text as comment_fonctionnel_column
          from  "COLUMNS_V2" c
      	  join "SDS" s using ("CD_ID")
      	  join "TBLS" t using ("SD_ID")
      	  join "DBS" d using ("DB_ID")
      	  left join "TABLE_PARAMS" numrow ON (numrow."TBL_ID" = t."TBL_ID" AND numrow."PARAM_KEY" = 'spark.sql.statistics.numRows')
      	  left join "TABLE_PARAMS" dt ON (dt."TBL_ID" = t."TBL_ID" AND dt."PARAM_KEY" = 'transient_lastDdlTime')
      	  left join "TABLE_PARAMS" dist ON (dist."TBL_ID" = t."TBL_ID" AND dist."PARAM_KEY" ~ 'spark.sql.statistics.colStats.\w+.distinctCount' AND dist."PARAM_KEY" ~ c."COLUMN_NAME")
      	  left join "TABLE_PARAMS" nc ON (nc."TBL_ID" = t."TBL_ID" AND nc."PARAM_KEY" ~ 'spark.sql.statistics.colStats.\w+.nullCount' AND nc."PARAM_KEY" ~ c."COLUMN_NAME")
      	  left join "TABLE_PARAMS" sch ON (sch."TBL_ID" = t."TBL_ID" AND sch."PARAM_KEY" ~ 'spark.sql.sources.schema.part.0')
         WHERE NOT ("TBL_TYPE" = 'EXTERNAL_TABLE' AND sch."PARAM_VALUE" is not null)
         """

  val SQL_HIVE_TABLE_EXT: String =
    """
       select
             '%s' lib_database,
             d."NAME" lib_schema,
             t."TBL_NAME" lib_table,
             "TBL_TYPE" typ_table,
             numrow."PARAM_VALUE"::bigint count_table,
             case when numrow."PARAM_VALUE" is null then null::timestamp else to_timestamp(cast(dt."PARAM_VALUE" as bigint))::timestamp end  last_analyze,
             "COMMENT" comment_fonctionnel,
             sch."PARAM_VALUE" as schem,
             sts.stts
          from  "COLUMNS_V2" c
      	  join "SDS" s using ("CD_ID")
      	  join "TBLS" t using ("SD_ID")
      	  join "DBS" d using ("DB_ID")
      	  left join "TABLE_PARAMS" numrow ON (numrow."TBL_ID" = t."TBL_ID" AND numrow."PARAM_KEY" = 'spark.sql.statistics.numRows')
      	  left join "TABLE_PARAMS" dt ON (dt."TBL_ID" = t."TBL_ID" AND dt."PARAM_KEY" = 'transient_lastDdlTime')
          left join (select "TBL_ID", array_agg("PARAM_KEY"||'='||"PARAM_VALUE") as stts from "TABLE_PARAMS" where "PARAM_KEY" ~ 'spark.sql.statistics.colStats.\w+.distinctCount|spark.sql.statistics.colStats.\w+.nullCount' group by "TBL_ID") sts
          on sts."TBL_ID" = t."TBL_ID"
      	  left join "TABLE_PARAMS" sch ON (sch."TBL_ID" = t."TBL_ID" AND sch."PARAM_KEY" ~ 'spark.sql.sources.schema.part.0')
         WHERE ("TBL_TYPE" = 'EXTERNAL_TABLE' AND sch."PARAM_VALUE" is not null)
         """

  val SQL_PG_VIEW: String =
    """
       select '%s' as lib_database,
      t.table_schema as lib_schema,
             t.table_name as lib_table,
             'view' as typ_table,
             null::bigint as count_table,
             null::timestamp as last_analyze,
             c.column_name lib_column,
             c.data_type typ_column,
             c.ordinal_position order_column
        from information_schema.tables t
      	 left join information_schema.columns c
      	     on t.table_schema = c.table_schema
      	     and t.table_name = c.table_name
       where table_type = 'VIEW'
         and t.table_schema not in ('information_schema', 'pg_catalog')
       order by 1,2,3, order_column
      """.stripMargin

  val SQL_PG_TABLE =
    """
       SELECT '%s' as lib_database,
             n.nspname AS lib_schema,
             c.relname AS lib_table,
             'physical' AS typ_table,
             co.rowcount AS count_table,
             co.last_analyze,
             f.attname::text AS lib_column,
             format_type(f.atttypid, f.atttypmod) AS typ_column,
             attnum::int as order_column,
             null_frac::float as null_ratio_column,
             n_distinct::bigint as count_distinct_column,
             obj_description(g.oid) as comment_fonctionnel,
             f.attnotnull AS is_mandatory,
             i.relname AS index_name,
             CASE
             WHEN i.oid <> 0::oid THEN 't'::text
             ELSE 'f'::text
             END AS is_index,
             CASE
             WHEN p.contype = 'p'::"char" THEN 't'::text
             ELSE 'f'::text
             END AS primarykey,
             CASE
             WHEN p.contype = 'u'::"char" THEN 't'::text
             WHEN p.contype = 'p'::"char" THEN 't'::text
             ELSE 'f'::text
             END AS uniquekey,
             col_description(('"'|| n.nspname || '"."' || c.relname ||'"')::regclass, attnum::int) as comment_fonctionnel_column
        FROM pg_attribute f
      	 JOIN pg_class c ON c.oid = f.attrelid
      	 JOIN pg_type t ON t.oid = f.atttypid
      	 LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum
      	 LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
      	 LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND (f.attnum = ANY (p.conkey))
      	 LEFT JOIN pg_class g ON p.confrelid = g.oid
      	 LEFT JOIN pg_index ix ON (f.attnum = ANY (ix.indkey::smallint[])) AND c.oid = f.attrelid AND c.oid = ix.indrelid
      	 LEFT JOIN pg_class i ON ix.indexrelid = i.oid
      	 left join (SELECT relname, schemaname, n_live_tup as rowcount, coalesce(last_autoanalyze, last_analyze) last_analyze FROM pg_stat_all_tables) co on (co.relname = c.relname and co.schemaname = n.nspname)
         left join pg_stats sts on (sts.schemaname = n.nspname AND sts.tablename = c.relname AND sts.attname = f.attname)
       WHERE c.relkind::text = 'r'::character(1)::text AND f.attnum > 0
         and n.nspname not in ('information_schema', 'pg_catalog')
       ORDER BY lib_schema, lib_table, order_column
       """
}

trait FeatureExtract {

  def extractSource(df: DataFrame): DataFrame = {
    df.withColumn("outil_source", regexp_extract(col("lib_table"), "^([^_]+)_.*$", 1))
      .withColumn("outil_source", when(col("outil_source").isin("orbis", "hegp", "mediweb", "phedra", "glims", "general", "arem", "ems"), col("outil_source")).otherwise("orbis"))
  }

  def extractPrimaryKey(df: DataFrame): DataFrame = {
    df.withColumn("is_pk", when((col("lib_column").rlike("^ids_")
      && col("order_column") === lit(1))
      || (col("lib_schema") === lit("coronaomop")
      && col("lib_column").rlike("_id$")
      && col("order_column") === lit(1))
      , lit(true))
      .otherwise(lit(false)))
  }

  def extractForeignKey(df: DataFrame): DataFrame = {
    df.withColumn("is_fk"
      , when((col("lib_column").rlike("^ids_")
        && col("order_column") =!= lit(1))
        || (col("lib_schema") === lit("coronaomop")
        && col("lib_column").rlike("_id$")
        && col("order_column") =!= lit(1)
        )
        , lit(true))
        .otherwise(lit(false)))
  }

  def inferForeignKey(df: DataFrame): DataFrame = {
    df.as("s")
      .filter(col("s.is_fk") === lit(true))
      .filter(!(col("s.lib_column") rlike "^ids_eds"))
      .join(df.as("t"), col("t.outil_source") === col("s.outil_source")
        && col("t.lib_table") =!= col("s.lib_table")
        && col("t.lib_schema") === col("s.lib_schema")
        && regexp_replace(col("t.lib_column"), "_ref_", "") === regexp_replace(col("s.lib_column"), "_ref_", "")
        && col("t.is_pk") === lit(true)
        , "left")
      .filter(col("t.lib_column").isNotNull)
      .union(
        df.as("s")
          .filter(col("s.is_fk") === lit(true))
          .filter(col("s.lib_schema") === lit("coronaomop"))
          .join(df.as("t"),
            col("t.lib_table") =!= col("s.lib_table")
              && col("t.lib_schema") === col("s.lib_schema")
              && regexp_replace(col("s.lib_column"), ".*_concept_id$|concept_id_\\d$", "concept_id") ===
              col("t.lib_column")
              && col("t.is_pk") === lit(true)
            , "left")
          .filter(col("t.lib_column").isNotNull))
      .selectExpr(
        "s.lib_database lib_database_source"
        , "s.lib_schema lib_schema_source"
        , "s.lib_table lib_table_source"
        , "s.lib_column lib_column_source"
        , "t.lib_database lib_database_target"
        , "t.lib_schema as lib_schema_target"
        , "t.lib_table as lib_table_target"
        , "t.lib_column as lib_column_target"
        , "'FK' as lib_reference")
  }

  def generateDatabase(df: DataFrame): DataFrame = {
    df.dropDuplicates("lib_database")
      .select("lib_database")
  }

  def generateSchema(df: DataFrame): DataFrame = {
    df.dropDuplicates("lib_database", "lib_schema")
      .withColumn("is_technical", when(expr(
        """
          (lib_database = 'pg-prod' AND lib_schema in ('eds'))
          OR  (lib_database = 'pg-prod' AND lib_schema in ('eds'))
          OR  (lib_database = 'eds-qua' AND lib_schema in ('eds'))
          OR  (lib_database = 'edsp-prod' AND lib_schema in ('edsp'))
          OR  (lib_database = 'spark-prod' AND lib_schema in ('edsprod', 'omop_prod', 'coronaomop'))
          OR  (lib_database = 'omop-prod' AND lib_schema in ('omop'))
          """)
        , lit(false))
        .otherwise(lit(true)))
      .select("lib_database", "lib_schema", "is_technical")
  }

  def generateTable(df: DataFrame): DataFrame = {
    df.dropDuplicates("lib_database", "lib_schema", "lib_table")
      .select("lib_database", "lib_schema", "lib_table", "outil_source", "count_table", "last_analyze", "typ_table")
  }

  def generateColumn(df: DataFrame): DataFrame = {
    val result = df.dropDuplicates("lib_database", "lib_schema", "lib_table", "lib_column")
      .selectExpr("lib_database", "lib_schema",
        "lib_table", "lib_column", "typ_column", "order_column",
        "is_mandatory", "is_pk", "is_fk",
        "null_ratio_column", "count_distinct_column",
        "comment_fonctionnel_column as comment_fonctionnel")
    result


  }
}
