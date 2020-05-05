package io.frama.parisni.spark.sync.compact

import com.typesafe.scalalogging.LazyLogging
import net.jcazevedo.moultingyaml._
import org.apache.spark.sql.SparkSession
import CompactTableYaml._

import scala.io.Source

object CompactTable extends App with LazyLogging {

  // Spark Session
  val spark = SparkSession.builder()
    .master("local")
    .appName("spark session")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val filename = args(0)
  val ymlTxt = Source.fromFile(filename).mkString
  val yaml = ymlTxt.stripMargin.parseYaml
  val database = yaml.convertTo[Database]

  val deltaPath = database.deltaPath
  val numFiles = database.numFiles
  val partition = database.partition.getOrElse("")
  val host = database.host.toString
  val port = database.port.toString
  val db = database.db.toString
  val user = database.user.toString
  val schema = database.schema.toString
  val pw = database.pw.getOrElse("")

  val compTab:CompactTable = new CompactTable
  compTab.compactTables(spark, deltaPath, partition, numFiles, host, port, user, schema, db, pw)
}


class CompactTable extends LazyLogging{

  //Table "meta_table"
  val META_TABLE_LIB = "meta_table"
  val SPARK_DB_LIB = "spark-prod"
  val DATABASE_LIB = "lib_database"
  val DELTA_TABLE_LIB = "lib_table"

  def compactTables(spark: SparkSession, deltaPath: String, partition: String = "", numFiles: Int, host:String, port:String, user:String,
                    schema:String, db:String, pw:String=""): Unit = {

    //URL to connect to DB
    val url = f"jdbc:postgresql://${host}:${port}/${db}?user=${user}&currentSchema=${schema}&password=${pw}"
    //logger.warn("URL = "+url)

    //Get list of delta tables
    val tables = spark.read.format("postgres")
      .option("url", url)
      .option("query", s"select ${DELTA_TABLE_LIB} from ${META_TABLE_LIB} where ${DATABASE_LIB} = '${SPARK_DB_LIB}'")
      .load.toDF

    //Show tables
    logger.warn("List of Delta Tables ---------------------")
    tables.show

    tables.take(tables.count.toInt).map(table => {

      val tableLib = table.get(0).toString
      //Compact table
      compactTable(spark, deltaPath, tableLib, partition, numFiles)
      logger.warn(s"Delta table ${deltaPath}/${tableLib} compacted successfully")
    })
  }

  def compactTable(spark: SparkSession, deltaPath: String, deltaTable: String, partition: String = "", numFiles: Int): Unit = {

    val fullPath = deltaPath + "/" + deltaTable
    //check if delta path exists
    if(!checkTableExists(spark, deltaPath, deltaTable)) {
      logger.warn(s"Delta Table ${fullPath} doesn't exists")
      return
    }

    if(partition.isEmpty)
      spark.read
        .format("delta")
        .load(fullPath)
        .repartition(numFiles)
        .write
        .option("dataChange", "false")
        .format("delta")
        .mode("overwrite")
        .save(fullPath)
    else{

      //val partition = "year = '2019'"
      val numFilesPerPartition = numFiles   //16

      spark.read
        .format("delta")
        .load(fullPath)
        .where(partition)
        .repartition(numFilesPerPartition)
        .write
        .option("dataChange", "false")
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", partition)
        .save(fullPath)
    }
  }

  def checkTableExists(spark: SparkSession, path: String, table: String): Boolean = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val deltaPath = path + "/" + table
    val res = fs.exists(new org.apache.hadoop.fs.Path(deltaPath))
    logger.warn(s"Delta Table ${deltaPath} exists = "+res)
    res
  }
}
