package io.frama.parisni.spark.quality

import org.apache.spark.sql.{DataFrame, QueryTest, SparkSession}

import scala.util.Try


class ConstraintsTest extends QueryTest with SparkSessionTestWrapper {
  import spark.implicits._
  import Constraints.{Schema, Field, ConstraintException}

  assume(! Constraints.skipped, "Constraint checking is skipped by environment variable")

  test("Check schema") {
    val df = List(
      ("value", 42)
    ).toDF("str", "num")
    Constraints.fromSchema(Schema(Seq(
      Field("str"),
      Field("num")
    )))(df)
  }

  test("Check success") {
    val df = List(
      ("value", 42)
    ).toDF("str", "num")
    val check = Constraints.fromSchema(Schema(Seq(
      Field("str", required = true, unique = true, allowed = Array("", "value")),
      Field("num", minimum = 0, maximum = 100d)
    )))
    assert(check(df).isSuccess)
  }

  test("Check failures") {
    // Failing each constrain exactly once
    val df = List(
      ("value", 42),
      ("nope", -1),
      ("_", 42),
      ("_", 101),
      (null.asInstanceOf[String], 42)
    ).toDF("str", "num")
    val check = Constraints.fromSchema(Schema(Seq(
      Field("str", required = true, unique = true, allowed = Array("_", "value"), pattern = "_|value"),
      Field("num", minimum = 0d, maximum = 100d)
    )))
    assertFailure(6, check(df))

    // Failing required on string and numeric
    // Null DF conversion only work for java Double
    val dfNull = List(
      ("value", 42d.asInstanceOf[java.lang.Double]),
      (null.asInstanceOf[String], null.asInstanceOf[java.lang.Double])
    ).toDF("str", "num")
    val checkNull = Constraints.fromSchema(Schema(Seq(
      Field("str", required = true),
      Field("num", required = true, minimum = 0, maximum = 100)
    )))
    // 4 fails because min & max also fail on NaN
    assertFailure(4, checkNull(dfNull))

    // Failing no nan on numeric
    val dfNan = List(
      (42d, 42d),
      (42d, Double.NaN)
    ).toDF("num1", "num2")
    val checkNan = Constraints.fromSchema(Schema(Seq(
      Field("num1", noNan = true),
      Field("num2", noNan = true, minimum = 0, maximum = 100)
    )))
    // TODO fail: should be 1 but NaN causes max to be NaN as well
    // https://github.com/awslabs/deequ/issues/224
    assertFailure(2, checkNan(dfNan))
  }

  test("Check ratio") {
    val df = List(
      ("value", 42),
      ("nope", -1),
      ("", 42)
    ).toDF("str", "num")
    val checkHalf = Constraints.fromSchema(Schema(Seq(
      Field("str", ratio = 0.5, allowed = Array("", "value")),
      Field("num", ratio = 0.5, minimum = 0)
    )))
    assert(checkHalf(df).isSuccess)
  }

  test("From Yaml") {
    val df = List(
      ("value", 42),
      ("nope", -1),
      ("_", 42),
      ("_", 101),
      (null.asInstanceOf[String], 42)
    ).toDF("str", "num")
    val check = Constraints.fromYaml(
      """
        |fields:
        |- name: str
        |  ratio: 0.99
        |  required: true
        |  unique: true
        |  allowed:
        |  -
        |  - value
        |  pattern: "_|value"
        |- name: num
        |  minimum: 0
        |  maximum: 100
        |""".stripMargin)
    assertFailure(6, check(df))
  }

  test("To Yaml") {
    val terse = Field("Terse")
    val verbose = Field("Verbose",
      ratio = 0.99,
      fatal = false,
      required = true,
      unique = true,
      pattern = "something",
      minimum = 0,
      maximum = 100,
      noNan = true,
      allowed = Array("to be", "not to be")
    )
    log.info(Constraints.schema2yaml(Schema(Seq(terse, verbose))))
    // only print non-default parameters
    assertResult("fields:\n- name: Terse\n"){
      Constraints.schema2yaml(Schema(Seq(terse)))
    }
    // all non-default parameters are serialized
    assertResult(Set("name", "ratio", "fatal", "required", "unique", "pattern", "minimum", "maximum", "noNan", "allowed")) {
      val Level2YamlKey = """^(?:- | {2})(\w+):.*""".r
      Constraints.schema2yaml(Schema(Seq(verbose))).lines.flatMap {
        case Level2YamlKey(key) => Some(key)
        case _ => None
      }.toSet
    }
  }

  private def assertFailure(expectedViolations: Int, result: Try[DataFrame]) = {
    assert(result.isFailure)
    assertResult(expectedViolations) {
      result.failed.get match {
        case e: ConstraintException => e.failed().size
        case _                      => -1
      }
    }
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
