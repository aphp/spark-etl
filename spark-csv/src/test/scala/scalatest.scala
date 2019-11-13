package io.frama.parisni.spark.csv

import org.apache.spark.sql.types._
import org.apache.spark.sql.{QueryTest, SparkSession}

class AppTest extends QueryTest with SparkSessionTestWrapper {


  test("test read csv1") {
    val mb = new MetadataBuilder()
    val m = mb.putString("default", "123").build
    val schema = StructType(
      StructField("c1", IntegerType)
        :: StructField("c2", IntegerType)
        :: StructField("c3", IntegerType, false, m)
        :: Nil)

    val inputDF = CSVTool(spark, "test1.csv", schema)

    val resultDF = spark.sql(
      """
      select cast(1 as int) as c1, cast(null as int) as c2, cast(123 as int) as c3 
      union all 
      select cast(null as int) as c1, cast(1 as int) as c2, cast(123 as int) as c3 
      """)
    val res = spark.createDataFrame(resultDF.rdd, schema)
    inputDF.show
    checkAnswer(inputDF, res)

  }
  test("test read csv2") {
    val mb = new MetadataBuilder()
    val m = mb.putString("default", "1515-01-01").build
    val schema = StructType(
      StructField("c1", IntegerType)
        :: StructField("c2", IntegerType)
        :: StructField("c3", DateType, false, m)
        :: Nil)

    val inputDF = CSVTool(spark, "test1.csv", schema)

    val resultDF = spark.sql(
      """
      select cast(1 as int) as c1, cast(null as int) as c2, cast('1515-01-01' as date) as c3 
      union all 
      select cast(null as int) as c1, cast(1 as int) as c2, cast('1515-01-01' as date) as c3 
      """)

    val res = spark.createDataFrame(resultDF.rdd, schema)
    inputDF.show
    checkAnswer(inputDF, res)

  }

  test("test get headers") {
    val original = Array("c1", "c2")
    val heads = CSVTool.getCsvHeaders(spark, "test1.csv", Some(","))
    assert(original.mkString == heads.mkString)
  }

  test("test get simple struct") {
    val schema = StructType(
      StructField("c1", StringType)
        :: StructField("c2", StringType)
        :: Nil)
    val res = CSVTool.getStringStructFromArray(CSVTool.getCsvHeaders(spark, "test1.csv", Some(",")))
    assert(schema.prettyJson == res.prettyJson)
  }

}

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

}
