SPARK-POSTGRES
==============

spark-postgres is a set of function to better bridge postgres and spark. It
focuses on stability and speed in ETL workloads.

Usage
*****

.. code-block:: scala
	
	import fr.aphp.eds.spark.postgres.PGUtil
	// the connection looks into /home/$USER/.pgpass for a password
	val url = "jdbc:postgresql://somehost:someport/somedb?user=someuser&currentSchema=someschema"
	// get a dataframe from a PGCOPY stmt
	val df = PGUtil.inputQueryBulkDf(spark, url, "select * from sometable", "/tmp/export.csv")
	// bulk load a temporary table and apply a SCD1 on sometable from the previous df with a default batch size of 50k rows
	PGUtil.outputBulkDfScd1(url, "sometable", "somekey", df)



Input from Postgres
*******************
- `inputQueryDf(spark:SparkSession, url:String,query:String,partition_column:String,num_partitions:Int):Dataset[Row]`
- `inputQueryBulkDf(spark:SparkSession, url:String, query:String, path:String, isMultiline:Boolean):Dataset[Row]`
- `inputQueryBulkCsv(conn:Connection, query:String, path:String)`

Ouput to Postgres
*****************
- `outputBulk(url:String, table:String, df:Dataset[Row], batchsize:Int)`

Slow Changing Dimension
***********************
- `outputBulkDfScd1(url:String, table:String, key:String, df:Dataset[Row], batchsize:Int)`
- `outputBulkDfScd2(url:String, table:String, key:String, dateBegin:String, dateEnd:String, df:Dataset[Row], batchsize:Int)`


Table functions
***************
- tableCreate
- tableCopy
- tableTruncate
- tableDrop

General functions
*****************
- `connOpen(url:Str):Connection`


