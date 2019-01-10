package fr.aphp.eds.spark.postgres

import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileOutputStream
import java.sql._
import java.util.Properties
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._
import org.postgresql.copy.{CopyManager,PGCopyInputStream}
import org.postgresql.core.BaseConnection;
import scala.util.control.Breaks._
import org.apache.spark.sql.SparkSession
import java.io.{BufferedInputStream, BufferedOutputStream, FileInputStream, FileOutputStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.util.UUID.randomUUID
import scala.reflect.io.Directory
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

class ExactPartitioner[V](partitions: Int) extends Partitioner {
  def getPartition(key: Any): Int = return math.abs(key.asInstanceOf[Int] % numPartitions())
  def numPartitions() : Int = partitions
}

class PGUtil(spark:SparkSession, url: String, tmpPath:String) {
  private var password: String = ""

  def setPassword(pwd:String = "") : PGUtil = {
    password = PGUtil.passwordFromConn(url, pwd)
    this
  }

  def showPassword() : Unit = {
    println(password)
  }

  private def genPath() :String = {
   tmpPath + "/"  + randomUUID.toString
  }

  def purgeTmp():Boolean = {
    val defaultFSConf = spark.sessionState.newHadoopConf().get("fs.defaultFS")
    val fsConf = if( tmpPath.startsWith("file:") ){ "file:///" }else{ defaultFSConf }
    val conf = new Configuration()
    conf.set("fs.defaultFS", fsConf)
    val fs= FileSystem.get(conf)
    fs.delete(new Path(tmpPath), true) // delete file, true for recursive 
  }

  def tableCreate(table:String, schema: StructType, isUnlogged:Boolean = false): PGUtil = {
  PGUtil.tableCreate(url, table, schema, isUnlogged, password)
  this
  }

  def tableCopy(tableSrc:String, tableTarg:String, isUnlogged:Boolean = true): PGUtil = {
  PGUtil.tableCopy(url, tableSrc, tableTarg, password, isUnlogged)
  this
  }

  def tableTruncate(table:String): PGUtil = {
  PGUtil.tableTruncate(url, table, password)
  this
  }

  def tableDrop(table:String): PGUtil = {
  PGUtil.tableDrop(url, table, password)
  this
  }

  def inputBulk(query:String, isMultiline:Boolean = false, numPartitions:Int=1, partitionColumn:String=""):Dataset[Row]={
  PGUtil.inputQueryBulkDf(spark, url, query, genPath, isMultiline, numPartitions, partitionColumn, password=password)
  }

  def outputBulk(table:String, df:Dataset[Row]): PGUtil = {
  PGUtil.outputBulkCsv(spark, url, table, df, genPath, password)
  this
  }

  def input(query:String, numPartitions:Int=1, partitionColumn:String=""):Dataset[Row]={
  PGUtil.inputQueryDf(spark, url, query, numPartitions, partitionColumn, password)
  }

  def output(table:String, df:Dataset[Row], batchsize:Int = 50000):PGUtil = {
  PGUtil.output(url, table, df, batchsize, password) 
  this
  }

  def outputScd1(table:String, key:String, df:Dataset[Row], numPartitions:Int = 4, excludeColumns:List[String] = Nil):PGUtil = {
  PGUtil.outputBulkDfScd1(spark, url, table, key, df, numPartitions, excludeColumns, genPath, password)
  this
  }

  def outputScd2(table:String, key:String, dateBegin:String, dateEnd:String, df:Dataset[Row], numPartitions:Int = 4, excludeColumns:List[String] = Nil):PGUtil = {
  PGUtil.outputBulkDfScd2(spark, url, table, key, dateBegin, dateEnd, df, numPartitions, excludeColumns, genPath, password)
  this
  }

}

object PGUtil extends java.io.Serializable {

   private def dbPassword(hostname:String, port:String, database:String, username:String ):String = {
    // Usage: val thatPassWord = dbPassword(hostname,port,database,username)
    // .pgpass file format, hostname:port:database:username:password

    val fs = FileSystem.get(new java.net.URI("file:///"), new Configuration)
    val file = fs.open(new Path(scala.sys.env("HOME"), ".pgpass"))
    val content = Iterator.continually(file.readLine()).takeWhile(_ != null).mkString("\n")
    var passwd = ""
    content.split("\n").foreach{line =>
      val connCfg = line.split(":")
      if (hostname == connCfg(0)
        && port == connCfg(1)
        && database == connCfg(2)
        && username == connCfg(3)
      ) { 
        passwd = connCfg(4)
      }
    }
    file.close()
    passwd
  }

  def passwordFromConn(url:String, password:String):String = {
    if(!password.isEmpty){
      return(password)
    }
    val pattern = "jdbc:postgresql://(.*):(\\d+)/(\\w+)[?]user=(\\w+).*".r
    val pattern(host, port, database, username) = url
    dbPassword(host,port,database,username)
  }

  def connOpen(url:String, password:String = ""):Connection = {
    val prop = new Properties()
    prop.put("password", passwordFromConn(url, password))
    val dbc: Connection = DriverManager.getConnection(url, prop)
    dbc
  }

  private def getSchemaTable(spark:SparkSession, url:String, table:String):StructType={
    val query = s""" select * from $table """ 
    getSchemaQuery(spark, url, query)
  }

  private def getSchemaQuery(spark:SparkSession, url:String, query:String, password:String = ""):StructType={
    val queryStr = s"""($query LIMIT 0) as tmp"""
    spark.read.format("jdbc")
      .option("url",url)
      .option("password",passwordFromConn(url, password))
      .option("driver","org.postgresql.Driver")
      .option("dbtable", queryStr)
      .load.schema
  }

  def tableTruncate(url:String, table:String, password:String = ""):Unit ={
    val conn = connOpen(url, password)
    val st: PreparedStatement = conn.prepareStatement(s"TRUNCATE TABLE $table")
    st.executeUpdate()
    conn.close()
  }

  def tableDrop(url:String, table:String, password:String = ""):Unit ={
    val conn = connOpen(url, password)
    val st: PreparedStatement = conn.prepareStatement(s"DROP TABLE IF EXISTS $table")
    st.executeUpdate()
    conn.close()
  }

  def tableCreate(url:String, table:String, schema:StructType, isUnlogged:Boolean, password:String = ""):Unit ={
    val conn = connOpen(url, password)
    val unlogged = if(isUnlogged){"UNLOGGED"}else{""}
    val fields = "" //TODO
    val queryCreate = s"""CREATE TABLE $unlogged ($fields)"""
    val st: PreparedStatement = conn.prepareStatement(queryCreate)
    st.executeUpdate()
    conn.close()
  }

  def tableCopy(url:String, tableSrc:String, tableTarg:String, password:String = "", isUnlogged:Boolean = true):Unit ={
    val conn = connOpen(url, password)
    val unlogged = if(isUnlogged){"UNLOGGED"}else{""}
    val queryCreate = s"""CREATE $unlogged TABLE $tableTarg (LIKE $tableSrc)"""
    val st: PreparedStatement = conn.prepareStatement(queryCreate)
    st.executeUpdate()
    conn.close()
  }

  private def getMinMaxForColumn(spark:SparkSession, url:String, query:String, partitionColumn:String, password: String = ""):Tuple2[Long,Long]={
    val min_max_query = s"(SELECT cast(min($partitionColumn) as bigint), cast(max($partitionColumn) as bigint) FROM $query) AS tmp1"
    val row  = spark.read.format("jdbc")
	.option("url",url)
	.option("driver","org.postgresql.Driver")
	.option("dbtable",min_max_query)
        .option("password",passwordFromConn(url, password))
        .load.first
    val lowerBound = row.getLong(0)
    val upperBound = row.getLong(1)
    (lowerBound, upperBound)
  }

  private def getPartitions(spark:SparkSession, lowerBound:Long, upperBound:Long, numPartitions:Int):RDD[Tuple2[Int, String]]={
    val length = BigInt(1) + upperBound - lowerBound
    import spark.implicits._
    val partitions = (0 until numPartitions).map { i =>
      val start = lowerBound + ((i * length) / numPartitions)
      val end = lowerBound + (((i + 1) * length) / numPartitions) - 1
      f"between $start AND $end"
    }.zipWithIndex.map{case(a, index) => (index, a)}.toDS.rdd.partitionBy(new ExactPartitioner(numPartitions))
    partitions
  }

  def inputQueryDf(spark:SparkSession, url:String, query:String,numPartitions:Int, partitionColumn:String,password:String = ""):Dataset[Row]={
    // get min and max for partitioning
    val queryStr = s"($query) as tmp"
    val (lowerBound, upperBound): Tuple2[Long,Long] = getMinMaxForColumn(spark, url, queryStr, partitionColumn)
    // get the partitionned dataset from multiple jdbc stmts
    spark.read.format("jdbc")
        .option("url",url)
	.option("dbtable",queryStr)
	.option("driver","org.postgresql.Driver")
	.option("partitionColumn",partitionColumn)
        .option("lowerBound",lowerBound)
        .option("upperBound",upperBound)
        .option("numPartitions",numPartitions)
        .option("fetchsize",50000)
        .option("password",passwordFromConn(url, password))
        .load
    }
  
  def outputBulkCsv(spark:SparkSession, url:String, table:String, df:Dataset[Row], path:String, password:String = "") = {
    //write a csv folder
    df.write.format("csv")
    .option("delimiter",",")
    .option("header",false)
    .option("emptyValue","")
    .option("quote","\"")
    .option("escape","\"")
    .mode(org.apache.spark.sql.SaveMode.Overwrite)
    .save(path)

    // load the csv files from hdfs in parallel 
    val fs = FileSystem.get(new Configuration())
    import spark.implicits._
    val rdd = fs.listStatus(new Path(path))
    .filter(x => x.getPath.toString.endsWith(".csv"))
    .map(x => x.getPath.toString).toList.zipWithIndex.map{case(a,i) => (i,a)}
    .toDS.rdd.partitionBy(new ExactPartitioner(8))

    rdd.foreachPartition(
      x => { 
      val conn = connOpen(url, password)
        x.foreach{ 
          s => {
          val stream = (FileSystem.get(new Configuration())).open(new Path(s._2)).getWrappedStream
          val copyManager: CopyManager = new CopyManager(conn.asInstanceOf[BaseConnection] );
          copyManager.copyIn(s"""COPY $table  FROM STDIN WITH CSV DELIMITER ',' ESCAPE '"' QUOTE '"' """, stream  );
          }
        }
      conn.close()
      x.toIterator
      })
  }

  def output(url:String, table:String, df:Dataset[Row], batchsize:Int = 50000, password:String = "") = {
    df.coalesce(8).write.mode(org.apache.spark.sql.SaveMode.Overwrite)
      .format("jdbc")
      .option("url",url)
      .option("dbtable",table)
      .option("batchsize",batchsize)
      .option("password",passwordFromConn(url, password))
      .option("driver","org.postgresql.Driver")
      .save()
  }
  
  def inputQueryPartBulkCsv(spark:SparkSession, fsConf:String, url:String, query:String, path:String, numPartitions:Int, partitionColumn:String, password:String="") = {
    val queryStr = s"($query) as tmp"
    val (lowerBound, upperBound) = getMinMaxForColumn(spark, url, queryStr, partitionColumn)
    val rdd = getPartitions(spark, lowerBound, upperBound, numPartitions)

    val tmp = rdd.foreachPartition(
      x => { 
      val conn = connOpen(url, password)
        x.foreach{ 
          s => {
          val queryPart = s"SELECT * FROM $queryStr WHERE $partitionColumn ${s._2}"
          inputQueryBulkCsv(fsConf, conn, queryPart, path)
          }
        }
      conn.close()
      x.toIterator
      })
  } 

  def inputQueryBulkCsv(fsConf:String, conn:Connection, query:String, path:String) = {
    val sqlStr = s""" COPY ($query) TO STDOUT  WITH DELIMITER AS ',' NULL AS '' CSV  ENCODING 'UTF-8' QUOTE '"' ESCAPE '"' """
    val copyInputStream: PGCopyInputStream  = new PGCopyInputStream(conn.asInstanceOf[BaseConnection],sqlStr)

    val conf = new Configuration()
    conf.set("fs.defaultFS", fsConf)
    val fs= FileSystem.get(conf)
    val output = fs.create(new Path(path , "part-" + randomUUID.toString + ".csv"))

    var flag = true
    while(flag){
      val t = copyInputStream.read()
      if(t > 0){ 
        output.write(t);
        output.write(copyInputStream.readFromCopy());
      }else{
        output.close()
        flag = false}
    }
  }

  def inputQueryBulkDf(spark:SparkSession, url:String, query:String, path:String, isMultiline:Boolean = false, numPartitions:Int=1, partitionColumn:String="", password:String = ""):Dataset[Row]={
    val defaultFSConf = spark.sessionState.newHadoopConf().get("fs.defaultFS")
    val fsConf = if( path.startsWith("file:") ){ "file:///" }else{ defaultFSConf }

    val conf = new Configuration()
    conf.set("fs.defaultFS", fsConf)
    val fs= FileSystem.get(conf)
    fs.delete(new Path(path), true) // delete file, true for recursive 

    val schemaQuery = getSchemaQuery(spark, url, query, password)
    if(numPartitions == 1){
    val conn = connOpen(url, password)
      inputQueryBulkCsv(fsConf, conn, query, path)
    conn.close
    }else{
      inputQueryPartBulkCsv(spark, fsConf, url, query, path, numPartitions, partitionColumn, password)
    }

    // read the resulting csv
    spark.read.format("csv")
      .schema(schemaQuery)
      .option("multiline",isMultiline)
      .option("delimiter",",")
      .option("header",false)
      .option("quote","\"")
      .option("escape","\"")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .option("dateFormat", "yyyy-MM-dd")
      .option("mode","FAILFAST")
      .load(path)
  }

  def outputBulkDfScd1(spark:SparkSession, url:String, table:String, key:String, df:Dataset[Row], batchsize:Int = 50000, excludeColumns:List[String] = Nil, path:String, password:String = ""):Unit ={
    val tableTmp = table + "_tmp"
    tableDrop(url, tableTmp, password)
    tableCopy(url, table, tableTmp, password)
    outputBulkCsv(spark, url, tableTmp, df, path)
    scd1(url, table, tableTmp, key, df.schema, excludeColumns, password)
    tableDrop(url, tableTmp, password)
  }

  def scd1(url:String, table:String, tableTarg:String, key:String, rddSchema:StructType, excludeColumns:List[String] = Nil, password:String = ""):Unit ={
    val conn = connOpen(url, password)
    val updSet =  rddSchema.fields.filter(x => !key.equals(x.name)).filter(x => !excludeColumns.contains(x.name)).map(x => s"${x.name} = tmp.${x.name}").mkString(",")
    val updIsDistinct =  rddSchema.fields.filter(x => !key.equals(x.name)).filter(x => !excludeColumns.contains(x.name)).map(x => s"tmp.${x.name} IS DISTINCT FROM tmp.${x.name}").mkString(" OR ")
    val upd = s"""
    UPDATE $table as targ
    SET $updSet
    FROM $tableTarg as tmp
    WHERE TRUE
    AND targ.$key = tmp.$key
    AND ($updIsDistinct)
    """
    conn.prepareStatement(upd).executeUpdate()

    val insSet =  rddSchema.fields.filter(x => !excludeColumns.contains(x.name)).map(x => s"${x.name}").mkString(",")
    val insSetTarg =  rddSchema.fields.filter(x => !excludeColumns.contains(x.name)).map(x => s"tmp.${x.name}").mkString(",")
    val ins = s"""
    INSERT INTO $table ($insSet)
    SELECT $insSetTarg
    FROM $tableTarg as tmp
    LEFT JOIN $table as targ USING ($key)
    WHERE TRUE
    AND targ.$key IS NULL
    """
    conn.prepareStatement(ins).executeUpdate()
    conn.close()
  }

  def outputBulkDfScd2(spark:SparkSession, url:String, table:String, key:String, dateBegin:String, dateEnd:String, df:Dataset[Row], batchsize:Int = 50000, excludeColumns:List[String] = Nil, path:String, password:String = ""):Unit ={
    val tableTmp = table + "_tmp"
    tableDrop(url, tableTmp, password)
    tableCopy(url, table, tableTmp, password)
    outputBulkCsv(spark, url, tableTmp, df, path)
    scd2(url, table, tableTmp, key, dateBegin, dateEnd, df.schema, excludeColumns, password)
    tableDrop(url, tableTmp, password)
  }

  def scd2(url:String, table:String, tableTmp:String, key:String, dateBegin:String, dateEnd:String, rddSchema:StructType, excludeColumns:List[String] = Nil, password:String = ""):Unit ={
    val conn = connOpen(url, password)
    val insCols =    rddSchema.fields.filter(x => x.name!=dateBegin).filter(x => !excludeColumns.contains(x.name)).map(x => s"${x.name}").mkString(",") + "," + dateBegin
    val insColsTmp = rddSchema.fields.filter(x => x.name!=dateBegin).filter(x => !excludeColumns.contains(x.name)).map(x => s"tmp.${x.name}").mkString(",") + ", now()"

    // update  where key AND  is distinct from
    // insert  where key AND  is distinct from
    val oldRows = s"""
    WITH
    upd AS (
    UPDATE $table c
    SET $dateEnd = now()
    FROM $tableTmp tmp
    WHERE TRUE
    AND tmp.$key  = c.$key 
    AND c.$dateEnd IS null
    AND tmp.concept_code IS DISTINCT FROM c.concept_code
    RETURNING c.$key
    )
    INSERT INTO $table ($insCols)
    SELECT $insColsTmp 
    FROM $tableTmp tmp
    JOIN upd USING ($key)
    """
    conn.prepareStatement(oldRows).executeUpdate()

    // insert  where key AND left join null
    val newRows = s"""
    INSERT INTO $table ($insCols)
    SELECT $insColsTmp 
    FROM $tableTmp tmp
    LEFT JOIN $table c USING ($key)
    WHERE c.$key IS null;
    """
    conn.prepareStatement(newRows).executeUpdate()
    conn.close()
  }
}
