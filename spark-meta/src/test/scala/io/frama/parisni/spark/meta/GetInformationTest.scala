package io.frama.parisni.spark.meta

import extractor.{DefaultFeatureExtractImpl, FeatureExtractTrait}
import org.apache.spark.sql.functions.{col, lit, regexp_extract, when}
import org.apache.spark.sql.{DataFrame, QueryTest}

class GetInformationTest extends QueryTest
  with SparkSessionTestWrapper {

  val featureExtract:FeatureExtractTrait = new DefaultFeatureExtractImpl

  test("is 'is_index' in 'meta_column' table") {
    val pgTableCsvPath: String = getClass.getResource("/meta/postgres-table.csv").getPath

    var pgTable: DataFrame = spark.read.format("csv").option("header", "true").load(pgTableCsvPath)
    pgTable = pgTable
      .withColumn("null_ratio_column", lit(0))
      .withColumn("count_distinct_column",lit(0))
      .withColumn("comment_fonctionnel_column",lit(""))
    pgTable.printSchema()

    //Replay the transformation dataflow
    pgTable = featureExtract.extractPrimaryKey(pgTable)
    pgTable = featureExtract.extractForeignKey(pgTable)
    pgTable = featureExtract.generateColumn(pgTable)

    pgTable.filter("is_index=='f'").show(3)
    pgTable.filter("is_index=='t'").show(3)

    assert(pgTable.columns.contains("is_index"))
  }


}


/*
  test("test get postgres view") {

    val csv = getClass.getResource("/meta/spark-table.csv").getPath
    //assert(CSVTool.getCsvHeaders(spark, csv, None).mkString(",") === "lib_database,lib_schema,lib_table,typ_table,lib_column,typ_column,order_column")

    val pgView = CSVTool.read(spark, csv, delimiter = None, escape = None, multiline = None, dateFormat = None, timestampFormat = None).cache

    // extraire la source
    var res = extractSource(pgView)

    // extraire la pk
    res = extractPrimaryKey(res)

    // extraire les fk
    res = extractForeignKey(res)


    val database = generateDatabase(res).show
    val schema = generateSchema(res).show
    val table = generateTable(res).show
    val column = generateColumn(res).show

    val reference = inferForeignKey(res)
    reference.show

  }

 */

