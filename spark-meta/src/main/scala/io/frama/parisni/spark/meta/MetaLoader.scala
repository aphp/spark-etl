package io.frama.parisni.spark.meta

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame

class MetaLoader(host: String, database: String, schema: String, user: String) extends LazyLogging {

  /**
   * Return a Dataframe as the left jointure between an input dataframe and the result of an SQL query
   *
   * @param   df
   *          input dataframe
   *
   * @param options
   *        a Map containing the SQL query and the jointure keys
   *
   * @return The jointure as a Dataframe
   */
  def getLookup(df: DataFrame, options: Map[String, String]): DataFrame = {

    val joinColumns = options("joinColumns").split(",")
    val pgJoinSql = options("pgJoinSql")

    // get the information from postgres
    val joinTable = df.sparkSession.read.format("io.frama.parisni.spark.postgres")
      .option("query", pgJoinSql)
      .option("host", host)
      .option("user", user)
      .option("database", database)
      .option("schema", schema)
      .option("partitions", 1)
      .option("multiline", value = true)
      .load
    // join to extend the hive table
    df.alias("h")
      .join(joinTable.as("t"), joinColumns.toSeq, "left")
    //df.printSchema
    //df
  }

  /**
   * Writing Dataframe into postgres table
   * SCD1 writing type means "add new or update"
   */
  def writeScd1(df: DataFrame, table: String, joinKey: String, filter: String, deleteSet: String): Unit = {
    logger.warn("loading scd1 with %d rows".format(df.count))
    df.write.format("io.frama.parisni.spark.postgres")
      .option("type", "scd1")
      .option("partitions", 4)
      .option("host", host)
      .option("user", user)
      .option("database", database)
      .option("schema", schema)
      .option("table", table)
      .option("joinKey", joinKey)
      .option("filter", filter)
      .option("deleteSet", deleteSet)
      .save
  }

  def loadDatabase(df: DataFrame, dbName: String): Unit = {
    writeScd1(df, "meta_database", "lib_database", s"lib_database = '$dbName'", "is_active = false")
  }

  def loadSchema(df: DataFrame, dbName: String): Unit = {
    val opt = Map("joinColumns" -> "lib_database"
      , "pgJoinSql" -> "select ids_database, lib_database from meta_database")
    val result = getLookup(df, opt)
    writeScd1(result, "meta_schema", "lib_database,lib_schema", s"lib_database = '$dbName'", "is_active = false")
  }

  def loadTable(df: DataFrame, dbName: String): Unit = {
    val opt = Map("joinColumns" -> "lib_database,lib_schema"
      , "pgJoinSql" -> "select ids_schema, d.lib_database, s.lib_schema from meta_schema s join meta_database d using (ids_database)")
    val result = getLookup(df, opt)
    writeScd1(result, "meta_table", "lib_database,lib_schema,lib_table", s"lib_database = '$dbName'", "is_active = false")
  }

  def loadColumn(df: DataFrame, dbName: String): Unit = {
    val opt = Map(
      "joinColumns" -> "lib_database,lib_schema,lib_table"
      , "pgJoinSql" -> "select ids_table, d.lib_database, s.lib_schema, t.lib_table from meta_table t join meta_schema s using (ids_schema) join meta_database d using (ids_database)"
    )
    val result = getLookup(df, opt)
    writeScd1(result, "meta_column", "lib_database,lib_schema,lib_table,lib_column", s"lib_database = '$dbName'", "is_active = false")
  }

  def loadReference(df: DataFrame, dbName: String): Unit = {
    val optSource = Map("joinColumns" -> "lib_database_source,lib_schema_source,lib_table_source,lib_column_source"
      , "pgJoinSql" ->
        "select ids_column as ids_source, d.lib_database as lib_database_source, s.lib_schema as lib_schema_source, t.lib_table as lib_table_source, c.lib_column as lib_column_source from meta_column c join meta_table t using (ids_table) join meta_schema s using (ids_schema) join meta_database d using (ids_database)")
    val resultSource = getLookup(df, optSource)

    val optTarget = Map("joinColumns" -> "lib_database_target,lib_schema_target,lib_table_target,lib_column_target"
      , "pgJoinSql" -> "select ids_column as ids_target, d.lib_database as lib_database_target, s.lib_schema as lib_schema_target, t.lib_table as lib_table_target, c.lib_column as lib_column_target from meta_column c join meta_table t using (ids_table) join meta_schema s using (ids_schema) join meta_database d using (ids_database)")
    val resultTarget = getLookup(resultSource, optTarget)

    writeScd1(resultTarget, "meta_reference", "lib_database_source,lib_schema_source,lib_table_source,lib_column_source,lib_database_target,lib_schema_target,lib_table_target,lib_column_target,lib_reference"
      , s"lib_database_source = '$dbName'", "is_active = false")
  }
}

