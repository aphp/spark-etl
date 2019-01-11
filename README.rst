SPARK-POSTGRES
==============

spark-postgres is a set of function to better bridge postgres and spark. It
focuses on stability and speed in ETL workloads. In particular it provides
access to the postgres bulk load function (COPY)

Supported version
+++++++++++++++++
- spark scala V2+ in yarn or local mode
- postgres v9+

Supported fields
++++++++++++++++
- numerics (int, bigint, float...)
- strings
- dates, timestamps
- array (TODO)

Use the lib
+++++++++++

To compile the code, clone it and use maven to build the shaded jar into the target folder.

- `mvn install`

The lib need the postgresql jdbc driver. You can download it from the
postgresql website. The lib works either in local mode, in yarn mode and has
been tested with apache livy.

- `spark-shell --driver-class-path /opt/lib/postgresql-42.2.5.jar  --jars "postgresql-42.2.5.jar,spark-postgres-2.0.1-SNAPSHOT-shaded.jar"  --master yarn`

Usage
+++++
.. code-block:: scala
	
	import fr.aphp.eds.spark.postgres.PGUtil
	// the connection looks into /home/$USER/.pgpass for a password
	val url = "jdbc:postgresql://somehost:someport/somedb?user=someuser&currentSchema=someschema"

        val pg = new PGUtil(sparkSession, url, "spark-postgres-tmp" ) // specify a temporary folder in hdfs or locally
        val df = pg
          .tableDrop("person_tmp") // drop table if exists
          .tableCopy("person","person_tmp") // duplicate the table without data
          .inputBulk(query="select * from person",  numPartitions=4, partitionColumn="person_id") // get a df from the table

        pg.outputBulk("person_tmp", df) // load the new table with the df
          .tableDrop("person_tmp") // drop the temparary table
          .purgeTmp() // purge the temporary folder


Functions:
++++++++++

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

Benchmark
+++++++++

Input
******
TODO

Output
******
TODO
