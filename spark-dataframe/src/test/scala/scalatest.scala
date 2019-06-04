
package io.frama.parisni.spark.dataframe

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
val dfTool = DFTool
  test("test reorder") {

    val inputDF = sqlContext.sql("select 1 as c2, 2 as c1")
    val schema = StructType(StructField("c1", IntegerType) :: StructField("c2", IntegerType) :: Nil)
    val resultDF = sqlContext.sql("select 2 as c1, 1 as c2")

    assertDataFrameEquals(resultDF, DFTool.reorderColumns(inputDF, schema))

  }

  test("test cast") {

    val inputDF = sqlContext.sql("select '2' as c1, '1' as c2")
    val schema = StructType(StructField("c1", IntegerType, false) :: StructField("c2", IntegerType, false) :: Nil)
    val resultDF = sqlContext.sql("select 2 as c1, 1 as c2")
    val testDF = DFTool.castColumns(inputDF, schema)
    assertDataFrameEquals(resultDF, testDF)

  }

  test("test columns missing") {

    val inputDF = sqlContext.sql("select 1 as c1, 2 as c2")
    val schema = StructType(StructField("c1", IntegerType) :: StructField("c2", IntegerType) :: Nil)

    DFTool.existColumns(inputDF, schema)

  }

  test("test mandatory columns") {
    val mb = new MetadataBuilder()
    val m = mb.putNull("default").build
    val schema = StructType(StructField("c1", IntegerType) :: StructField("c2", IntegerType, true, m) :: Nil)
    val mandatorySchema = StructType(StructField("c1", IntegerType) :: Nil)

    assert(DFTool.getMandatoryColumns(schema).toDDL == mandatorySchema.toDDL)

  }

  test("test optional columns") {
    val mb = new MetadataBuilder()
    val m = mb.putNull("default").build
    val schema = StructType(StructField("c1", IntegerType) :: StructField("c2", IntegerType, true, m) :: Nil)
    val optionalSchema = StructType(StructField("c2", IntegerType, true, m) :: Nil)

    assert(DFTool.getOptionalColumns(schema).toDDL == optionalSchema.toDDL)

  }

  test("test add columns") {
    val mb = new MetadataBuilder()
    val m = mb.putNull("default").build
    val optionalSchema = StructType(StructField("c3", IntegerType, true, m) :: Nil)
    val inputDF = sqlContext.sql("select '2' as c1, '1' as c2")
    val resultDF = sqlContext.sql("select '2' as c1, '1' as c2, cast(null as int) as c3")

    assertDataFrameEquals(DFTool.addMissingColumns(inputDF, optionalSchema), resultDF)

  }

  

}
