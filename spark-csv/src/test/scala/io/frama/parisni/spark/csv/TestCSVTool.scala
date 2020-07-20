package io.frama.parisni.spark.csv

import java.io.File
import java.nio.file.Files

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.types._

class TestCSVTool extends QueryTest with SparkSessionTestWrapper {

  test("test read csv1") {
    val mb = new MetadataBuilder()
    val m = mb.putString("default", "123").build
    val schema = StructType(
      StructField("c1", IntegerType)
        :: StructField("c2", IntegerType)
        :: StructField("c3", IntegerType, nullable = false, m)
        :: Nil
    )

    val inputDF = CSVTool(spark, "test1.csv", schema)

    val resultDF = spark.sql("""
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
        :: StructField("c3", DateType, nullable = false, m)
        :: Nil
    )

    val inputDF = CSVTool(spark, "test1.csv", schema)

    val resultDF = spark.sql("""
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
        :: Nil
    )
    val res = CSVTool.getStringStructFromArray(
      CSVTool.getCsvHeaders(spark, "test1.csv", Some(","))
    )
    assert(schema.prettyJson == res.prettyJson)
  }

  test("write to local") {
    import spark.implicits._
    val df = ((1, 2, 3) :: (2, 3, 4) :: Nil).toDF("a", "b", "c").repartition(2)
    CSVTool.writeCsvLocal(df, "/tmp/testdf", "/tmp/result.csv")
    assert(spark.read.option("header", true).csv("/tmp/result.csv").count === 2)
  }

  test("test write file to local") {
    import spark.implicits._
    val df =
      ((1, "boby") :: (2, "jim") :: Nil).toDF("file", "content").repartition(2)
    val path = Files.createTempDirectory("result")
    CSVTool.writeDfToLocalFiles(
      df,
      "file",
      "content",
      path.toAbsolutePath.toString
    )
    assert(new File(path.toString).list().length === 2)
  }

}
