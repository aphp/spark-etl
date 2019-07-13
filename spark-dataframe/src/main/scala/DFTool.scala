package io.frama.parisni.spark.dataframe

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{ hash, col, lit, row_number }
import org.apache.spark.sql.expressions.Window
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types.IntegerType

/** Factory for [[io.frama.parisni.spark.dataframe.DFTool]] instances. */
object DFTool extends LazyLogging {

  /**
   * Apply a schema on the given DataFrame. It reorders the
   *  columns, cast them, validates the non-nullable columns.
   *
   *  @param df their name
   *  @param schema the schema as a StructType
   *  @return a validated DataFrame
   *
   */
  def applySchema(df: DataFrame, schema: StructType): DataFrame = {
    val mandatoryColumns = DFTool.getMandatoryColumns(schema)
    val optionalColumns = DFTool.getOptionalColumns(schema)

    existColumns(df, mandatoryColumns)
    val dfWithoutCol = removeBadColumns(df, schema)
    val dfWithCol = addMissingColumns(dfWithoutCol, optionalColumns)
    val dfReorder = reorderColumns(dfWithCol, schema)
    val result = castColumns(dfReorder, schema)

    result
  }

  /**
   * Apply a schema on the given DataFrame. It reorders the
   *  columns.
   *
   *  @param df their name
   *  @param schema the schema as a StructType
   *  @return a validated DataFrame
   *
   */
  def reorderColumns(df: DataFrame, schema: StructType): DataFrame = {
    val reorderedColumnNames = schema.fieldNames
    df.select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)
  }

  /**
   * Apply a schema on the given DataFrame. It casts the columns.
   *
   *  @param df their name
   *  @param schema the schema as a StructType
   *  @return a validated DataFrame
   *
   */
  def castColumns(df: DataFrame, schema: StructType): DataFrame = {
    val newDf = validateNull(df, schema)
    val trDf = newDf.schema.fields.foldLeft(df) {
      (df, s) => df.withColumn(s.name, df(s.name).cast(s.dataType))
    }
    validateNull(trDf, schema)
  }

  /**
   * Apply a schema on the given DataFrame. It validates
   *  the non-null columns.
   *
   *  @param df their name
   *  @param schema the schema as a StructType
   *  @return a validated DataFrame
   *
   */
  def validateNull(df: DataFrame, schema: StructType): DataFrame = {
    df.sparkSession.createDataFrame(df.rdd, schema)

  }

  /**
   * Validate schema on the given DataFrame. It verifies if
   *  the columns exists independently on the schema.
   *
   *  @param df their name
   *  @param schema the schema as a StructType
   *  @return a validated DataFrame
   *
   */
  def existColumns(df: DataFrame, columnsNeeded: StructType) = {
    var tmp = ""
    val columns = df.columns
    for (column <- columnsNeeded.fieldNames) {
      if (!columns.contains(column))
        tmp += column + ", "
    }
    if (tmp != "") {
      throw new Exception(f"Missing columns in the data: [${tmp}]")
    }
  }

  /**
   * Look for mandatory columns within the schema.
   *
   *  @param schema : a StructType
   *  @return a StructType
   *
   */
  def getMandatoryColumns(schema: StructType): StructType = {
    StructType(schema.filter(f => !f.metadata.contains("default")))
  }

  /**
   * Look for optionnal columns within the schema.
   *
   *  @param schema : a StructType
   *  @return a StructType
   *
   */
  def getOptionalColumns(schema: StructType): StructType = {
    StructType(schema.filter(f => f.metadata.contains("default")))
  }

  /**
   * Add missing columns and apply the default value
   *  specified as a Metadata passed with the StrucType
   *
   *  @param df : a DataFrame
   *  @param missingSchema : StructType
   *  @return a DataFrame
   *
   */
  def addMissingColumns(df: DataFrame, missingSchema: StructType): DataFrame = {
    var result = df
    missingSchema.fields.foreach(
      f => {
        logger.debug(f"Added ${f.name} column")
        if (!df.columns.contains(f.name))
          result = result.withColumn(f.name, lit(f.metadata.getString("default")).cast(f.dataType))

      })
    result
  }
  /**
   *  Remove unspecified columns
   *
   *  @param df : a DataFrame
   *  @param schema : StructType
   *  @return a DataFrame
   *
   */
  def removeBadColumns(df: DataFrame, schema: StructType): DataFrame = {
    var result = df
    var dfSchema = df.schema
    dfSchema.fields.foreach(
      f => {
        logger.debug(f"Added ${f.name} column")
        if (!schema.fieldNames.contains(f.name))
          result = result.drop(f.name)
      })
    result
  }

  /**
   * Create an empty DataFrame accordingly to a schema.
   *
   *  @param spark: a SparkSession
   *  @param schema : a schema as a StructType
   *  @return a DataFrame
   *
   */
  def createEmptyDataFrame(spark: SparkSession, schema: StructType): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }

  /**
   * Remove rows from a DataFrame, given a specified
   *  colum
   *
   *  @param df: a DataFrame
   *  @param column: a String
   *  @return a DataFrameType
   *
   */
  def removeNullRows(df: DataFrame, column: String): DataFrame = {
    df.createOrReplaceTempView("nullTmp")
    val spark = df.sparkSession
    var nulltmp = spark.sql(f"select * from nullTmp where $column IS NULL")
    logger.warn(nulltmp.count + " missing rows")
    spark.sql(f"select * from nullTmp where $column IS NOT NULL and trim($column) !=''")
  }

  /**
   * Remove duplicates and show a report
   *
   *  @param df: a DataFrame
   *  @param colum: the columns not to be duplicated
   *  @return a DataFrameType
   *
   */
  def removeDuplicate(df: DataFrame, column: String*): DataFrame = {
    val tmp = df.dropDuplicates(column)

    val diff = df.count - tmp.count
    if (diff > 0) {
      println(f"removed $diff rows")
      df.except(tmp).show
    }
    tmp
  }

  /**
   * Adds a hash column based on several other columns
   *
   *  @param df DataFrame
   *  @param columnsToExclude List[String] the columns not to be hashed
   *  @return DataFrame
   *
   */
  def dfAddHash(df: DataFrame, columnsToExclude: List[String] = Nil): DataFrame = {

    df.withColumn("hash", hash(df.columns.filter(x => !columnsToExclude.contains(x)).map(x => col(x)): _*))

  }

  /**
   * Adds a hash column based on several other columns
   *
   *  @param df DataFrame
   *  @param columnsToExclude List[String] the columns not to be hashed
   *  @return DataFrame
   *
   */
  def dfAddSequence(df: DataFrame, columnName: String, indexBegin: Long = 0): DataFrame = {
    val firstCol = df.columns(0)

    val w = Window.partitionBy("fake").orderBy(col(firstCol))
    df
      .withColumn("fake", lit(1))
      .withColumn(columnName, row_number().over(w).plus(indexBegin))
      .drop("fake")
  }

  /**
   * Rename multiple columns
   *
   *  @param df DataFrame
   *  @param columns Map[String -> String]
   *  @return DataFrame
   *
   */
  def dfRenameColumn(df: DataFrame, columns: Map[String, String]): DataFrame = {
    var retDf = df
    columns.foreach({
      f => { retDf = retDf.withColumnRenamed(f._1, f._2) }
    })
    retDf
  }



}
