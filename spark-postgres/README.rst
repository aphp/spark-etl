SPARK-POSTGRES
==============

spark-postgres is a set of function to better bridge postgres and spark. It
focuses on stability and speed in ETL workloads. The API provides both a
regular **spark-datasource** (postgres), and several **useful functions**.

spark-postgres is designed for **large datasets**. It outperforms Apache Sqoop by
factor 8 for both reading/writing to postgres.

- use of pg COPY statements
- parallel writes with csv/binary stream methods
- parallel reads based on numeric/string key
- optional use of hdfs to store intermediary csv
- reindex after bulk-loading
- SCD1 computations done on the spark side
- use unlogged tables when needed
- prepared statements made easy
- spark datasource for high level API

spark-postgres is **reliable** and handles array types and also multiline text
columns.

It can be used from **scala-spark** and **pySpark**


Datasource Usage
++++++++++++++++
.. code-block:: scala
	
      // this fetch the query with 4 threads and produces a spark dataframe
      // this produces 5 * 4 = 20 files to be read one by one (multiline)
      val data = spark.read.format("postgres")
      .option("query","select * from myTable")     
      .option("partitions",4)
      .option("numSplits",5)
      .option("multiline",true)
      .option("host","localhost")
      .option("database","myDb")
      .option("schema","mySchema")
      .option("user","myUser")
      .option("partitionColumn","id")
      .load

      // optionaly use jdbc url
      val data = spark.read.format("postgres")
      .option("url","jdbc:postgres://localhost:5432/postgres?user=postgres&currentSchema=public")
      .option("password","myUnsecuredPassword")     
      .option("query","select * from myTable")     
      .option("partitions",4)
      .option("numSplits",5)
      .option("multiline",true)
      .option("partitionColumn","id")
      .load

      // this copy the spark dataframe into postgres with 4 threads
      // also it disable the indexes and reindexes the table afteward
      import spark.implicits._
      (1::2::3::Nil).toDF("id")
      .write.format("postgres")
      .option("type","full")
      .option("partitions",4)
      .option("reindex",true)
      .option("table","thePgTable")     
      .option("host","localhost")
      .option("database","myDb")
      .option("deleteSet","delete_date = now()") // this update statement will be applied when the row is not in the candidate table anymore
      .option("user","myUser")
      .option("schema","mySchema")
      .mode(org.apache.spark.sql.SaveMode.Overwrite) // this drops the table and recreates it from scratch
      .save

      // this applies an optimized SCD1 from the spark dataframe into postgres with 4 threads
      // insert the new rows and update the modified based on a hash column (called hash)
      // based on the composite key bookId, clientId
      import spark.implicits._
      ((1,1,"bob")::(2,3,"jim")::Nil).toDF("bookId", "clienId", "content")
      .write.format("postgres")
      .option("type","scd1")
      .option("filter","thecol = 31") // only fetch the rows matching the predicate for scd1
      .option("partitions",4)
      .option("joinKey","bookId,clienId")
      .option("table","thePgTable")     
      .option("host","localhost")
      .option("user","myUser")
      .option("database","myDb")
      .option("schema","mySchema")
      .save

      // this applies an optimized SCD1 from the spark dataframe into postgres with 4 threads
      // insert the new rows and update the modified based on a hash column (called hash)
      // based on the composite key bookId, clientId.
      // Also, it only fetches from postgres the rows specified by the filter
      // and apply a deletion on rows not present in the spark side, by running the update statement within deleteSet
      import spark.implicits._
      ((1,1,"bob")::(2,3,"jim")::Nil).toDF("bookId", "clienId", "content")
      .write.format("postgres")
      .option("type","scd1")
      .option("partitions",4)
      .option("joinKey","bookId,clienId")
      .option("table","thePgTable")     
      .option("host","localhost")
      .option("kill-locks","true") // this will kill the query that would lock the table if droped
      .option("bulkLoadMode","csv") // choose the copy strategy (csv/stream)
      .option("user","myUser")
      .option("database","myDb")
      .option("filter","col = 'value' AND col2 = 1")
      .option("deleteSet","is_active = false, delete_date = now()")
      .option("schema","mySchema")
      .save
      
Low level API Scala
+++++++++++++++++++
.. code-block:: scala
	
	import io.frama.parisni.PGUtil
	// the connection looks into /home/$USER/.pgpass for a password
	val url = "jdbc:postgresql://somehost:someport/somedb?user=someuser&currentSchema=someschema"

        val pg = PGUtil(sparkSession, url, "spark-postgres-tmp" ) // specify a temporary folder in hdfs or locally
        val df = pg
          .tableDrop("person_tmp") // drop table if exists
          .tableCopy("person","person_tmp") // duplicate the table without data
          .inputBulk(query="select * from person",  numPartitions=4, partitionColumn="person_id") // get a df from the table

        pg.outputBulk("person_tmp", df, numPartitions=4) // load the new table with the df with 4 thread
          .sqlExec("UPDATE logs set active = true")
          .tableDrop("person_tmp") // drop the temparary table
          .purgeTmp() // purge the temporary folder

	// exemple for multiline textual content
	// isMultiline allow the csv reader not to crash
	// splitFactor allow creating more csv, to increase paralleism for reading
        val df_multi = pg
          .tableDrop("note_tmp") // drop table if exists
          .tableCopy("note","note_tmp") // duplicate the table without data
          .inputBulk(query="select * from note",  isMultiline=true, numPartitions=4, splitFactor=10, partitionColumn="note_id") // get a df from the table

Low level API pySpark
+++++++++++++++++++++

.. code-block:: python

    url = "jdbc:postgresql://somehost:someport/somedb?user=someuser&currentSchema=someschema"
    pg = sc._jvm.io.frama.parisni.PGUtil.apply(spark._jsparkSession, url, "/tmp/")
    pg.inputBulk("select * from test2",False, 1, 1, "col").show()
    pg.purgeTmp()

Supported version
+++++++++++++++++
- spark scala V2.4+ in yarn or local mode
- postgres v9+

Supported fields
++++++++++++++++
- numerics (int, bigint, float...)
- strings (included multiline strings)
- dates, timestamps
- boolean
- array[] (int, double, string...)

Compile
+++++++

To compile the code, clone it and use maven to build the shaded jar into the target folder.

- `mvn install`

The lib need the postgresql jdbc driver. You can download it from the
postgresql website. The lib works either in local mode, in yarn mode and has
been tested with apache livy.

- `spark-shell --driver-class-path postgresql-42.2.5.jar  --jars "postgresql-42.2.5.jar,spark-postgres-2.3.0-SNAPSHOT-shaded.jar"  --master yarn`
