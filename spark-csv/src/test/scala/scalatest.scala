package io.frama.parisni.spark.csv

import org.scalatest.FunSpec
import org.scalatest.BeforeAndAfterAll
import scala.collection.mutable.Stack
import com.holdenkarau.spark.testing._
import com.holdenkarau.spark.testing.DataFrameSuiteBase._
import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import scala.transient
import org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED

class AppTest extends FunSuite with SharedSparkContext with DataFrameSuiteBase {

  override implicit def reuseContextIfPossible: Boolean = true

 

  test("test read csv1") {
    val mb = new MetadataBuilder()
    val m = mb.putString("default", "123").build
    val schema = StructType(
      StructField("c1", IntegerType)
        :: StructField("c2", IntegerType)
        :: StructField("c3", IntegerType, false, m)
        :: Nil)

    val inputDF = CSVTool(spark, "test1.csv", schema)

    val resultDF = sqlContext.sql("""
      select cast(1 as int) as c1, cast(null as int) as c2, cast(123 as int) as c3 
      union all 
      select cast(null as int) as c1, cast(1 as int) as c2, cast(123 as int) as c3 
      """)
    val res = spark.createDataFrame(resultDF.rdd, schema)
    inputDF.show
    assertDataFrameEquals(inputDF, res)

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

    val resultDF = sqlContext.sql("""
      select cast(1 as int) as c1, cast(null as int) as c2, cast('1515-01-01' as date) as c3 
      union all 
      select cast(null as int) as c1, cast(1 as int) as c2, cast('1515-01-01' as date) as c3 
      """)
      
    val res = spark.createDataFrame(resultDF.rdd, schema)
    inputDF.show
    assertDataFrameEquals(inputDF, res)

  }

}
