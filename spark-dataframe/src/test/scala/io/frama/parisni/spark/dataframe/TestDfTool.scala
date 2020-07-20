package io.frama.parisni.spark.dataframe

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class TestDfTool extends QueryTest with SparkSessionTestWrapper {

  val dfTool = DFTool
  test("test reorder") {

    val inputDF = spark.sql("select 1 as c2, 2 as c1")
    val schema = StructType(
      StructField("c1", IntegerType) :: StructField("c2", IntegerType) :: Nil
    )
    val resultDF = spark.sql("select 2 as c1, 1 as c2")

    checkAnswer(resultDF, DFTool.reorderColumns(inputDF, schema))

  }

  test("test cast") {

    val inputDF = spark.sql("select '2' as c1, '1' as c2")
    val schema = StructType(
      StructField("c1", IntegerType, nullable = false) :: StructField(
        "c2",
        IntegerType,
        nullable = false
      ) :: Nil
    )
    val resultDF = spark.sql("select 2 as c1, 1 as c2")
    val testDF = DFTool.castColumns(inputDF, schema)
    checkAnswer(resultDF, testDF)

  }

  test("test columns missing") {

    val inputDF = spark.sql("select 1 as c1, 2 as c2")
    val schema = StructType(
      StructField("c1", IntegerType) :: StructField("c2", IntegerType) :: Nil
    )

    DFTool.existColumns(inputDF, schema)

  }

  test("test mandatory columns") {
    val mb = new MetadataBuilder()
    val m = mb.putNull("default").build
    val schema = StructType(
      StructField("c1", IntegerType, nullable = false) :: StructField(
        "c2",
        IntegerType,
        nullable = true,
        m
      ) :: Nil
    )
    val mandatorySchema = StructType(StructField("c1", IntegerType) :: Nil)

    assert(DFTool.getMandatoryColumns(schema).toDDL == mandatorySchema.toDDL)

  }

  test("test optional columns") {
    val mb = new MetadataBuilder()
    val m = mb.putNull("default").build
    val schema = StructType(
      StructField("c1", IntegerType, nullable = false) :: StructField(
        "c2",
        IntegerType,
        nullable = true,
        m
      ) :: Nil
    )
    val optionalSchema =
      StructType(StructField("c2", IntegerType, nullable = true, m) :: Nil)

    assert(DFTool.getOptionalColumns(schema).toDDL == optionalSchema.toDDL)

  }

  test("test add columns") {
    val mb = new MetadataBuilder()
    val m = mb.putNull("default").build
    val optionalSchema =
      StructType(StructField("c3", IntegerType, nullable = true, m) :: Nil)
    val inputDF = spark.sql("select '2' as c1, '1' as c2")
    val resultDF =
      spark.sql("select '2' as c1, '1' as c2, cast(null as int) as c3")

    checkAnswer(DFTool.addMissingColumns(inputDF, optionalSchema), resultDF)

  }

  test("test rename columns") {
    val mb = new MetadataBuilder()
    val m = mb.putNull("default").build
    val inputDF = spark.sql("select '2' as c1, '1' as c2")
    val resultDF = spark.sql("select '2' as bob, '1' as jim")

    val columns = Map("c1" -> "bob", "c2" -> "jim")

    checkAnswer(DFTool.dfRenameColumn(inputDF, columns), resultDF)

  }

  test("generate case classes") {
    val struct = StructType(StructField("bob", IntegerType) :: Nil)
    val test = spark.sql("select 1 as bob, 2 as jim")
    val result = spark.sql("select 1 as bob")
    val testDF = DFTool.applySchema(test, struct)
    val resultDF = DFTool.applySchema(result, struct)
    checkAnswer(testDF, resultDF)
  }

  test("test pivot") {
    val df = spark.sql("""
       select 1 group, 'bob' key, 'so' value 
       union all
    		 select 2 group, 'jim' key, 'tore' value 
       """)
    DFTool
      .simplePivot(
        df,
        col("group"),
        col("key"),
        "array(value)",
        "bob" :: "jim" :: Nil
      )
      .show
  }

  test("test special character columns") {
    val struct = StructType(StructField("bob.jim", IntegerType) :: Nil)
    val test = spark
      .sql("select 1 as bob, 2 as jim")
      .withColumn("bob.jim", col("bob"))
    val result = spark.sql("select 1 as `bob.jim`")
    val testDF = DFTool.applySchemaSoft(test, struct)
    val resultDF = DFTool.applySchemaSoft(result, struct)
    checkAnswer(testDF, resultDF)
  }

  test("test union heterogeneous tables") {
    import spark.implicits._
    val right = List(
      (1, 2, 3),
      (4, 5, 6)
    ).toDF("a", "b", "c")

    val left = List(
      (1, 2, 3),
      (4, 5, 6)
    ).toDF("a", "b", "d")

    DFTool.unionDataFrame(left, right)

  }

  test("test trim all string") {

    import spark.implicits._
    val df = List(
      (" b ", " ", 3),
      ("a\n", " c", 6)
    ).toDF("a", "b", "d")
    val expectedDf = List(
      ("b", null, 3),
      ("a", "c", 6)
    ).toDF("a", "b", "d")

    checkAnswer(DFTool.trimAll(df), expectedDf)
  }

  test("test normalize cols") {

    import spark.implicits._
    val df = List(
      (" b ", " ", 3),
      ("a\n", " c", 6)
    ).toDF("Bob", "Ji mé", "(hi)")
    val expectedDf = Array("bob", "ji_me", "_hi_")

    assert(DFTool.normalizeColumnNames(df).columns === expectedDf)
  }

  test("test normalize dates") {

    import spark.implicits._
    val df = List(
      ("2020-01-02", " ", 3),
      ("a\n", " c", 6)
    ).toDF("dt", "Ji m", "(hi)")
    df.withColumn("dt", DFTool.toDate(col("dt"), "yyyy-MM-dd")).show
  }

  test("get archived tables") {
    import spark.implicits._
    val newDf = List((1, 2, 3), (4, 5, 6)).toDF("id", "cd", "value")
    val old1 = List((1, 2, 4), (7, 8, 9)).toDF("id", "cd", "value")
    val old2 = List((1, 2, 5), (7, 10, 12)).toDF("id", "cd", "value")

    val result = List((1, 2, 3), (4, 5, 6), (7, 8, 9)).toDF("id", "cd", "value")

    checkAnswer(result, DFTool.getArchived(Seq("id"), newDf, old1, old2))
  }

  test("get empty archived ") {
    import spark.implicits._
    val newDf = List((1, 2, 3), (4, 5, 6)).toDF("id", "cd", "value")
    val old1 = List((1, 2, 4), (7, 8, 9)).toDF("id", "cd", "value")
    val old2 = List((1, 2, 5), (7, 10, 12)).toDF("id", "cd", "value")

    val result = List((1, 2, 3), (4, 5, 6)).toDF("id", "cd", "value")

    checkAnswer(result, DFTool.getArchived(Seq("id"), newDf))
  }

  test("test scd1 delta") {
    import spark.implicits._
    val newDf = List((1, 2, 3), (4, 5, 6)).toDF("id", "cd", "value")
    val newDf2 = List((1, 2, 3), (5, 5, 8)).toDF("id", "cd", "value")
    val newDf3 = List((1, 2, 3), (4, 5, 6), (5, 5, 8)).toDF("id", "cd", "value")
    DFTool.deltaScd1(newDf, "testTable", List("id"), "default")

    checkAnswer(spark.table("testTable"), dfTool.dfAddHash(newDf))

    DFTool.deltaScd1(newDf, "testTable", List("id"), "default")

    checkAnswer(spark.table("testTable"), dfTool.dfAddHash(newDf))

    DFTool.deltaScd1(newDf2, "testTable", List("id"), "default")

    checkAnswer(spark.table("testTable"), dfTool.dfAddHash(newDf3))
  }

  test("test scd1 delta duplicated") {
    import spark.implicits._
    val newDf = List((143169167L, 2, 3)).toDF("id", "cd", "value")
    val newDf2 = List((143169167L, 2, 4)).toDF("id", "cd", "value")

    DFTool.deltaScd1(newDf, "testTable2", List("id"), "default")

    checkAnswer(spark.table("testTable2"), dfTool.dfAddHash(newDf))

    DFTool.deltaScd1(newDf2, "testTable2", List("id"), "default")

    checkAnswer(spark.table("testTable2"), dfTool.dfAddHash(newDf2))
  }
}
