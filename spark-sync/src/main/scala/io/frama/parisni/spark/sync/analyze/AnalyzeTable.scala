package io.frama.parisni.spark.sync.analyze

import com.typesafe.scalalogging.LazyLogging
import io.frama.parisni.spark.postgres.PGTool
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.SparkSession
import AnalyzeTableYaml._

import scala.io.Source

object AnalyzeTable extends App with LazyLogging {

  val filename = args(0)
  val ymlTxt = Source.fromFile(filename).mkString
  val yaml = ymlTxt.stripMargin.parseYaml
  val database = yaml.convertTo[Database]

  // Spark Session
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("spark session")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val deltaPath = database.deltaPath
  val host = database.host.toString
  val port = database.port.toString
  val db = database.db.toString
  val user = database.user.toString
  val schema = database.schema.toString
  val pw = database.pw.getOrElse("")

  val analyze: AnalyzeTable = new AnalyzeTable
  analyze.analyzeTables(spark, deltaPath, host, port, user, schema, db, pw)
}

class AnalyzeTable extends LazyLogging {

  def analyzeTables(
      spark: SparkSession,
      deltaPath: String,
      host: String,
      port: String,
      user: String,
      schema: String,
      db: String,
      pw: String = ""
  ): Unit = {

    import spark.implicits._

    //URL to connect to DB
    val url =
      f"jdbc:postgresql://${host}:${port}/${db}?user=${user}&currentSchema=${schema}&password=${pw}"

    //Table "meta_column" in DB "spark-prod"
    val dbSparkLib = "spark-prod"
    val tableColumns = "meta_column"
    val tableTables = "meta_table"
    val columnLibField = "lib_column"
    val tableIdField = "ids_table"
    val tableLibField = "lib_table"
    val dbLibField = "lib_database"
    val lastUpdateField = "last_analyze"

    //Get list of tables of DB "spark-prod"
    val tables = spark.read
      .format("postgres")
      .option("url", url)
      .option(
        "query",
        s"select ${tableIdField}, ${tableLibField} from ${tableTables} where ${dbLibField} = '${dbSparkLib}'"
      )
      .load
      .toDF

    //Show tables
    logger.warn("Tables of DB 'spark-prod'")
    tables.show

    tables
      .as[MetaTable]
      .take(tables.count.toInt)
      .map(table => {

        val tableId = table.idsTable
        val tableName = table.libTable

        //Get list of columns of a given table: col1 [, col2, ...]
        val columns = spark.read
          .format("postgres")
          .option("url", url)
          .option(
            "query",
            s"select ${columnLibField} from ${tableColumns} where ${tableIdField} = '${tableId}'"
          )
          .load
          .map(x => x.get(0).toString)
          .collect
          .mkString(", ")

        //Show columns of this table
        logger.warn(s"Columns of table ${tableName}: " + columns)

        //Analyze table and columns
        analyzeCols(spark, deltaPath, tableName, columns)

        //Update date of last_analyze
        val query =
          s"update ${tableTables} set ${lastUpdateField} = now() where ${tableIdField} = '${tableId}'"
        logger.warn("query = " + query)
        PGTool.sqlExec(url, query)

        //Show table after update
        val tabDFAfter = spark.read
          .format("postgres")
          .option("url", url)
          .option(
            "query",
            s"select * from ${tableTables} where ${tableIdField} = '${tableId}'"
          )
          .load
          .toDF
        tabDFAfter.show
      })
  }

  def analyzeCols(
      spark: SparkSession,
      path: String,
      table: String,
      columns: String
  ): Unit = {
    val tablePath = s"${path}/${table}"
    spark.sql(s"CREATE TABLE ${table} USING DELTA LOCATION '${tablePath}'")
    val query =
      s"analyze table ${table} compute statistics for columns ${columns}"
    logger.warn("query = " + query)
    spark.sql(query)
  }
}

case class MetaTable(idsTable: Int, libTable: String)
