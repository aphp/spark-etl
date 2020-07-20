package io.frama.parisni.spark.quality

import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.checks.{Check, CheckLevel, CheckResult, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import com.typesafe.scalalogging.LazyLogging
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.DataFrame

import scala.io.Source
import scala.util.{Failure, Success, Try}

object Constraints extends LazyLogging with DefaultYamlProtocol {

  case class Field(
      name: String,
      ratio: Double = 1d,
      fatal: Boolean = true,
      required: Boolean = false, // Note only works on nulls, not NaN
      unique: Boolean = false,
      pattern: String =
        "", // Note: matching empty string will fail anyway, implemented with regexp_extract != ""
      minimum: Double = Double.NegativeInfinity,
      maximum: Double =
        Double.PositiveInfinity, // Note: NaN fails maximum, https://github.com/awslabs/deequ/issues/224
      noNan: Boolean = false,
      allowed: Array[String] = Array.empty
  ) {
    val level: CheckLevel.Value =
      if (fatal) CheckLevel.Error else CheckLevel.Warning
    val assertion: Double => Boolean = _ >= ratio
    def mkCheck(msg: String): Check = Check(level, s"$name constraint on $msg")
    lazy val checks: Seq[Check] = Seq(
      if (required) Some(mkCheck("Required").hasCompleteness(name, assertion))
      else None,
      if (unique) Some(mkCheck("Unique").hasUniqueness(name, assertion))
      else None,
      if (!pattern.isEmpty)
        Some(mkCheck(s"/$pattern/").hasPattern(name, pattern.r, assertion))
      else None,
      if (!minimum.isNegInfinity)
        Some(
          mkCheck(s">=$minimum")
            .isGreaterThanOrEqualTo(name, minimum.toString, assertion)
        )
      else None,
      if (!maximum.isPosInfinity)
        Some(
          mkCheck(s">=$maximum")
            .isLessThanOrEqualTo(name, maximum.toString, assertion)
        )
      else None,
      if (noNan)
        Some(
          mkCheck("No NaN")
            .satisfies(s"not isnan(`$name`)", s"NotNaN($name)", assertion)
        )
      else None,
      if (allowed.nonEmpty)
        Some(
          mkCheck(allowed.mkString("In(", ", ", ")"))
            .isContainedIn(name, allowed, assertion)
        )
      else None
    ).flatten
  }

  case class Schema(fields: Seq[Field]) {
    def checks: Seq[Check] = fields.flatMap(_.checks)
  }

  implicit object FieldFormat extends YamlFormat[Field] {
    override def read(yaml: YamlValue): Field =
      yaml.asYamlObject.getFields(YamlString("name")) match {
        case Seq(YamlString(name)) =>
          val fields = yaml.asYamlObject.fields
          def get[A](key: String, default: A) =
            fields
              .get(YamlString(key))
              .map {
                case a: A => a
                case _    => deserializationError(s"Unexpected format for $key")
              }
              .getOrElse(default)
          Field(
            name,
            ratio = get("ratio", YamlNumber(1d)).convertTo[Double],
            fatal = get("fatal", YamlBoolean(true)).boolean,
            required = get("required", YamlBoolean(false)).boolean,
            unique = get("unique", YamlBoolean(false)).boolean,
            pattern = get("pattern", YamlString("")).value,
            minimum = get("minimum", YamlNumber(Double.NegativeInfinity))
              .convertTo[Double],
            maximum = get("maximum", YamlNumber(Double.PositiveInfinity))
              .convertTo[Double],
            noNan = get("noNan", YamlBoolean(false)).boolean,
            allowed =
              get("allowed", YamlArray(Vector.empty[YamlString])).elements.map {
                case YamlNull => ""
                case o        => o.convertTo[String]
              }.toArray
          )
        case _ => deserializationError("Expected name for Field")
      }

    override def write(field: Field): YamlValue =
      YamlObject(
        Seq(
          Some("name" -> YamlString(field.name)),
          if (field.ratio != 1.0d) Some("ratio" -> YamlNumber(field.ratio))
          else None,
          if (!field.fatal) Some("fatal" -> YamlBoolean(false)) else None,
          if (field.required) Some("required" -> YamlBoolean(true)) else None,
          if (field.unique) Some("unique" -> YamlBoolean(true)) else None,
          if (!field.pattern.isEmpty)
            Some("pattern" -> YamlString(field.pattern))
          else None,
          if (!field.minimum.isNegInfinity)
            Some("minimum" -> YamlNumber(field.minimum))
          else None,
          if (!field.maximum.isPosInfinity)
            Some("maximum" -> YamlNumber(field.maximum))
          else None,
          if (field.noNan) Some("noNan" -> YamlBoolean(true)) else None,
          if (field.allowed.nonEmpty)
            Some(
              "allowed" ->
                YamlArray(
                  field.allowed.map(YamlString(_).asInstanceOf[YamlValue]): _*
                )
            )
          else None
        ).flatten.map {
          case (k, v) => YamlString(k).asInstanceOf[YamlValue] -> v
        }.toMap
      )
  }
  val schemaFormat: YamlFormat[Schema] = yamlFormat1(Schema)
  def yaml2Schema(yaml: String): Schema = yaml.parseYaml.convertTo(schemaFormat)
  def schema2yaml(schema: Schema): String =
    schemaFormat.write(schema).prettyPrint

  class ConstraintException(val result: VerificationResult)
      extends IllegalArgumentException("DataFrame violated constraints") {
    def failed(fatalOnly: Boolean = false): Iterator[CheckResult] = {
      val filter =
        if (fatalOnly) (_: CheckResult).status == CheckStatus.Error
        else (_: CheckResult).status != CheckStatus.Success
      result.checkResults.valuesIterator.filter(filter)
    }
  }

  def apply(checks: Check*)(df: DataFrame): Try[DataFrame] = {
    val result = VerificationSuite().onData(df).addChecks(checks).run()
    val (loggable, ret) = result.status match {
      case CheckStatus.Success => (false, Success(df))
      case CheckStatus.Warning => (true, Success(df))
      case CheckStatus.Error   => (true, Failure(new ConstraintException(result)))
    }
    if (loggable) {
      for (
        (check, ress) <- result.checkResults;
        res <- ress.constraintResults if res.status != ConstraintStatus.Success
      ) {
        val message =
          s"${res.constraint} failed: ${res.message.getOrElse("Unknown failure")}"
        if (check.level == CheckLevel.Error) logger.error(message)
        else logger.warn(message)
      }
    }
    ret
  }

  // Environment variable to disable all but manual checks
  val ENV_VAR_SKIP = "SKIP_QUALITY_CONSTRAINTS"
  lazy val skipped: Boolean =
    !Option(System.getenv(ENV_VAR_SKIP)).forall(_.isEmpty)

  def skippable(
      doIt: => DataFrame => Try[DataFrame]
  ): DataFrame => Try[DataFrame] =
    if (skipped) Success(_) else doIt

  def fromSchema(schema: Schema): DataFrame => Try[DataFrame] =
    skippable {
      val checks = schema.checks
      apply(checks: _*)
    }

  def fromYaml(schemaYaml: String): DataFrame => Try[DataFrame] =
    skippable {
      fromSchema(yaml2Schema(schemaYaml))
    }

  def fromYamlFile(schemaYamlPath: String): DataFrame => Try[DataFrame] =
    skippable {
      val source = Source.fromFile(schemaYamlPath)
      try {
        fromYaml(source.mkString)
      } finally {
        source.close()
      }
    }

}
