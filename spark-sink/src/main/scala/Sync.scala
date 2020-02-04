import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

class Sync extends LazyLogging{

  def syncSourceTarget(spark: SparkSession, config: Map[String, String], dates: List[String],
                       pks: List[String]): Unit = {

    val sourceType = config.get("S_TABLE_TYPE").getOrElse("")
    val targetType = config.get("T_TABLE_TYPE").getOrElse("")
    var sourceDF = spark.emptyDataFrame

    val pgc = new PostgresConf(config, dates, pks)
    val dc = new DeltaConf(config, dates, pks)
    val slc = new SolrConf(config, dates, pks)

    /** Calculation of date_max: be sure that "target" table exists
          (in order to calculate date_max) or provide date_max **/
    var date_max = ""
    if(targetType == "postgres")  date_max = pgc.getDateMax(spark)
    else if(targetType == "delta") date_max = dc.getDateMax(spark)
    else if(targetType == "solr") date_max = slc.getDateMax(spark)

    // Load table "source" as DF
    sourceType match{
      case "postgres" => {

        val host = pgc.getHost.getOrElse("localhost")
        val port = pgc.getPort.getOrElse("5432")
        val db = pgc.getDB.getOrElse("postgres")
        val user = pgc.getUser.getOrElse("postgres")
        val schema = pgc.getSchema.getOrElse("public")
        val s_table = pgc.getSourceTableName.getOrElse("")
        val s_date_field = pgc.getSourceDateField.getOrElse("")
        val load_type = pgc.getLoadType.getOrElse("")

        sourceDF = pgc.readSource(spark, host, port, db, user, schema, s_table, s_date_field, date_max, load_type)
        logger.warn("Source: Showing Postgres table")
        sourceDF.show()
      }
      case "delta" => {

        val path = dc.getPath.getOrElse("/tmp")
        val s_table = dc.getSourceTableName.getOrElse("")
        val s_date_field = dc.getSourceDateField.getOrElse("")
        val load_type = dc.getLoadType.getOrElse("")

        sourceDF = dc.readSource(spark, path, s_table, s_date_field, date_max, load_type)
        logger.warn("Source: Showing Delta table")
        sourceDF.show()
      }
      case "solr" => {

        val zkhost = slc.getZkHost.getOrElse("")
        val s_collection = slc.getSourceTableName.getOrElse("")
        val s_date_field = slc.getSourceDateField.getOrElse("")

        //@transient var cloudClient: CloudSolrClient = _
        //sourceDF = slc.readSource( null, spark, zkhost, s_collection, s_date_field, date_max)
        sourceDF = slc.readSource(spark, zkhost, s_collection, s_date_field, date_max)
        logger.warn("Source: Showing Solr collection")
        sourceDF.show()
      }
    }

    if(!sourceDF.isEmpty) {
      // Merge DF with table "target"
      targetType match {
        case "postgres" => {

          val host = pgc.getHost.getOrElse("localhost")
          val port = pgc.getPort.getOrElse("5432")
          val db = pgc.getDB.getOrElse("postgres")
          val user = pgc.getUser.getOrElse("postgres")
          val schema = pgc.getSchema.getOrElse("public")
          val t_table = pgc.getTargetTableName.getOrElse("")
          val load_type = pgc.getLoadType.getOrElse("")
          val hash_field = pgc.getSourcePK.mkString(",")

          pgc.writeSource(spark, sourceDF, host, port, db, user, schema, t_table, load_type, hash_field)
          logger.warn("Target: Showing Postgres table")

          val url = f"jdbc:postgresql://${host}:${port}/${db}?user=${user}&currentSchema=${schema}"
          spark.read.format("postgres")
            .option("url",url)
            .option("query", s"select * from ${t_table}")
            //.option("partitions",4)
            //.option("partitionColumn","id")
            //.option("numSplits",5)
            .load.show
        }
        case "delta" => {

          val path = dc.getPath.getOrElse("/tmp")
          val t_table = dc.getTargetTableName.getOrElse("")
          val load_type = dc.getLoadType.getOrElse("")

          dc.writeSource(spark, sourceDF, path, t_table, load_type)
          logger.warn("Target: Showing Delta table")
          spark.read.format("delta").load(path + "/" + t_table).show
        }
        case "solr" => {

          val zkhost = slc.getZkHost.getOrElse("")
          val t_collection = slc.getTargetTableName.getOrElse("")
          //val load_type = slc.getLoadType.getOrElse("full")   //always "full"

          slc.writeSource(sourceDF, zkhost, t_collection)
          logger.warn("Target: Showing Solr collection")
          val options = Map( "collection" -> t_collection, "zkhost" -> zkhost)
          spark.read.format("solr").options(options).load.show
        }
      }
    }
  }
}