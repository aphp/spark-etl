import DeltaToDelta.{database, spark}
import DeltaToDeltaYaml.Database
import com.typesafe.scalalogging.LazyLogging
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.SparkSession

import scala.io.Source

  object DeltaToDelta extends App with LazyLogging {

  val filename = args(0)
  val ymlTxt = Source.fromFile(filename).mkString
  val yaml = ymlTxt.stripMargin.parseYaml
  val database = yaml.convertTo[Database]

  // Spark Session
  val spark = SparkSession.builder()
    .appName(database.jobName)
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  try {

    val dateFieldDelta = database.timestampLastColumn.getOrElse("")
    val dateFieldsDelta = database.timestampColumns.getOrElse(List())
    val dateMax = database.dateMax.getOrElse("")

    for(table <- database.tables.getOrElse(Nil)) {
      //if (table.isActive.getOrElse(true)) {

        //val pathTarget = table.pathTarget.toString
        val s_table = table.tableDeltaSource.toString
        val path = table.path.toString
        val t_table = table.tableDeltaTarget.toString
        val loadType = table.typeLoad.getOrElse("full")
        val pks = table.key

        val config = Map("S_TABLE_NAME" -> s_table, "S_TABLE_TYPE" -> "delta", "S_DATE_FIELD" -> dateFieldDelta,
          "T_TABLE_NAME" -> t_table, "T_TABLE_TYPE" -> "delta",  "T_DATE_MAX" -> dateMax,
          "PATH" -> path, "T_LOAD_TYPE" -> loadType
        )

        val sync = new Sync()
        sync.syncSourceTarget(spark, config, dateFieldsDelta, pks)

      //}
    }
  }

  spark.close()
}


class DeltaToDelta2 extends App with LazyLogging {

  val filename = "deltaToDelta.yaml"     //args(0)
  val ymlTxt = Source.fromFile(filename).mkString
  val yaml = ymlTxt.stripMargin.parseYaml
  val database = yaml.convertTo[Database]

  // Spark Session
  val spark = SparkSession.builder()
    .appName(database.jobName)
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  def sync(spark: SparkSession, database:Database): Unit = {
    try {
        val dateFieldDelta = database.timestampLastColumn.getOrElse("")
        val dateFieldsDelta = database.timestampColumns.getOrElse(List())
        val dateMax = database.dateMax.getOrElse("")

        for(table <- database.tables.getOrElse(Nil)) {
          //if (table.isActive.getOrElse(true)) {

          //val pathTarget = table.pathTarget.toString
          val s_table = table.tableDeltaSource.toString
          val path = table.path.toString
          val t_table = table.tableDeltaTarget.toString
          val loadType = table.typeLoad.getOrElse("full")
          val pks = table.key

          val config = Map("S_TABLE_NAME" -> s_table, "S_TABLE_TYPE" -> "delta", "S_DATE_FIELD" -> dateFieldDelta,
            "T_TABLE_NAME" -> t_table, "T_TABLE_TYPE" -> "delta",  "T_DATE_MAX" -> dateMax,
            "PATH" -> path, "T_LOAD_TYPE" -> loadType
          )

          val sync = new Sync()
          sync.syncSourceTarget(spark, config, dateFieldsDelta, pks)

          //}
        }
    }
  }

  //spark.close()
}

