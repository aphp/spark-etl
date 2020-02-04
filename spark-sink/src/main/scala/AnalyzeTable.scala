import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.sql.{PreparedStatement, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import net.jcazevedo.moultingyaml._
import AnalyzeTableYaml.Database
import com.typesafe.scalalogging.LazyLogging
import io.frama.parisni.spark.dataframe.DFTool.logger
import io.frama.parisni.spark.postgres.PGTool

import scala.annotation.meta.getter
import scala.io.Source

object AnalyzeTable extends App with LazyLogging {

  val filename = args(0)
  val ymlTxt = Source.fromFile(filename).mkString
  val yaml = ymlTxt.stripMargin.parseYaml
  val database = yaml.convertTo[Database]

  // Spark Session
  val spark = SparkSession.builder()
    .master("local")
    .appName("spark session")
    //.config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val deltaPath = database.deltaPath
  val host = database.host.toString
  val port = database.port.toString
  val db = database.db.toString
  val user = database.user.toString
  val schema = database.schema.toString
  val pw = database.pw.getOrElse("")

  val analyze:AnalyzeTable = new AnalyzeTable
  analyze.analyzeTables(spark, deltaPath, host, port, user, schema, db, pw)
}


class AnalyzeTable extends LazyLogging{

  def analyzeTables(spark: SparkSession, deltaPath: String, host:String, port:String,
                    user:String, schema:String, db:String, pw:String=""): Unit = {

    import spark.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql._

    //URL to connect to DB
    val url = f"jdbc:postgresql://${host}:${port}/${db}?user=${user}&currentSchema=${schema}&password=${pw}"
    //logger.warn("URL = "+url)

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
    //val tables: List[MetaTable] = List()
    val tables = spark.read.format("postgres")
      .option("url", url)
      .option("query", s"select ${tableIdField}, ${tableLibField} from ${tableTables} where ${dbLibField} = '${dbSparkLib}'")
      .load.toDF

    //Show tables
    logger.warn("Tables of DB 'spark-prod'")
    tables.show

    tables.as[MetaTable].take(tables.count.toInt).map(table => {

      val tableId = table.ids_table
      val tableName = table.lib_table

      //Get list of columns of a given table: col1 [, col2, ...]
      val columns = spark.read.format("postgres")
        .option("url", url)
        .option("query", s"select ${columnLibField} from ${tableColumns} where ${tableIdField} = '${tableId}'")
        .load.map(x => x.get(0).toString)
        .collect.mkString(", ")

      //Show columns of this table
      logger.warn(s"Columns of table ${tableName}: "+columns)

      //Analyze table and columns
      analyzeCols(spark, deltaPath, tableName, columns)
      //analyzeColsPg(spark, db, tableName, columns, url)

      //Update date of last_analyze
      val query = s"update ${tableTables} set ${lastUpdateField} = now() where ${tableIdField} = '${tableId}'"
      logger.warn("query = "+query)
      PGTool.sqlExec(url, query)

      //Show table after update
      val tabDFAfter = spark.read.format("postgres")
        .option("url", url)
        .option("query", s"select * from ${tableTables} where ${tableIdField} = '${tableId}'")
        .load.toDF
      tabDFAfter.show
    })
  }

  def analyzeCols(spark: SparkSession, path:String, table: String, columns: String): Unit = {
    val tablePath = s"${path}/${table}"
    spark.sql(s"CREATE TABLE ${table} USING DELTA LOCATION '${tablePath}'")
    val query = s"analyze table ${table} compute statistics for columns ${columns}"
    logger.warn("query = "+query)
    spark.sql(query)
  }

  def analyzeColsPg(spark: SparkSession, db:String, table: String, columns: String, url: String): Unit = {
    val query = s"analyze ${table} (${columns})"
    logger.warn("query = "+query)
    PGTool.sqlExec(url, query)
  }
}

case class MetaTable(ids_table:Int, lib_table:String)

