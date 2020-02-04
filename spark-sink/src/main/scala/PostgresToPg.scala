import PostgresToPg.{database, spark}
import PostgresToPgYaml.Database
import com.typesafe.scalalogging.LazyLogging
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.SparkSession

import scala.io.Source

object PostgresToPg extends App with LazyLogging {

  val filename = args(0)
  val ymlTxt = Source.fromFile(filename).mkString
  val yaml = ymlTxt.stripMargin.parseYaml
  val database = yaml.convertTo[Database]

  // Spark Session
  val spark = SparkSession.builder()
    .appName(database.jobName)
    //.enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  try {

    val hostPg = database.hostPg.toString
    val portPg = database.portPg.toString
    val databasePg = database.databasePg.toString
    val userPg = database.userPg.toString
    val dateFieldSource = database.timestampLastColumn.getOrElse("")
    val dateFieldsTarget = database.timestampColumns.getOrElse(List())
    val dateMax = database.dateMax.getOrElse("")

    for(table <- database.tables.getOrElse(Nil)) {
      //if (table.isActive.getOrElse(true)) {

        val tablePgSource = table.tablePgSource.toString
        val tablePgTarget = table.tablePgTarget.toString
        val schemaPg = table.schemaPg.toString
        val loadType = table.typeLoad.getOrElse("full")
        val pks = table.key

        val config = Map("S_TABLE_NAME" -> tablePgSource, "S_TABLE_TYPE" -> "postgres",
          "S_DATE_FIELD" -> dateFieldSource, "HOST" -> hostPg, "PORT" -> portPg,
          "DATABASE" -> databasePg, "USER" -> userPg, "SCHEMA" -> schemaPg,

          "T_TABLE_NAME" -> tablePgTarget, "T_TABLE_TYPE" -> "postgres",
          "T_LOAD_TYPE" -> loadType, "T_DATE_MAX" -> dateMax
        )

        val sync = new Sync()
        sync.syncSourceTarget(spark, config, dateFieldsTarget, pks)

      //}
    }
  }
  spark.close()
}



class PostgresToPg2 extends App with LazyLogging{

  val filename = "postgresToPg.yaml"     //args(0)
  val ymlTxt = Source.fromFile(filename).mkString
  val yaml = ymlTxt.stripMargin.parseYaml
  val database = yaml.convertTo[Database]

  // SPARK PART
  val spark = SparkSession.builder()
    .appName(database.jobName)
    //.enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  def sync(spark: SparkSession, database: Database, port: String): Unit = {
    try {

      val hostPg = database.hostPg.toString
      val portPg = port   //database.portPg.toString
      val databasePg = database.databasePg.toString
      val userPg = database.userPg.toString
      val dateFieldSource = database.timestampLastColumn.getOrElse("")
      val dateFieldsTarget = database.timestampColumns.getOrElse(List())
      val dateMax = database.dateMax.getOrElse("")

      for(table <- database.tables.getOrElse(Nil)) {
        //if (table.isActive.getOrElse(true)) {

        val tablePgSource = table.tablePgSource.toString
        val tablePgTarget = table.tablePgTarget.toString
        val schemaPg = table.schemaPg.toString
        val loadType = table.typeLoad.getOrElse("full")
        val pks = table.key

        val config = Map("S_TABLE_NAME" -> tablePgSource, "S_TABLE_TYPE" -> "postgres",
          "S_DATE_FIELD" -> dateFieldSource, "HOST" -> hostPg, "PORT" -> portPg,
          "DATABASE" -> databasePg, "USER" -> userPg, "SCHEMA" -> schemaPg,

          "T_TABLE_NAME" -> tablePgTarget, "T_TABLE_TYPE" -> "postgres",
          "T_LOAD_TYPE" -> loadType, "T_DATE_MAX" -> dateMax
        )

        val sync = new Sync()
        sync.syncSourceTarget(spark, config, dateFieldsTarget, pks)

        //}
      }
    }
  }
  spark.close()
}
