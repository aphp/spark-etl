SPARK-POSTGRES
==============

spark-postgres is a set of function to better bridge postgres and spark. It
focuses on stability and speed in ETL workloads. In particular it provides
access to the postgres bulk load function (COPY) and also provides SQL access.

It can be used from **scala-spark** and **pySpark**


Supported version
+++++++++++++++++
- spark scala V2+ in yarn or local mode
- postgres v9+

Supported fields
++++++++++++++++
- numerics (int, bigint, float...)
- strings
- dates, timestamps
- boolean
- array[] (int, double, string...)

Use the lib
+++++++++++

To compile the code, clone it and use maven to build the shaded jar into the target folder.

- `mvn install`

The lib need the postgresql jdbc driver. You can download it from the
postgresql website. The lib works either in local mode, in yarn mode and has
been tested with apache livy.

- `spark-shell --driver-class-path postgresql-42.2.5.jar  --jars "postgresql-42.2.5.jar,spark-postgres-2.3.0-SNAPSHOT-shaded.jar"  --master yarn`

Usage Scala
+++++++++++
.. code-block:: scala
	
	import fr.aphp.eds.spark.postgres.PGUtil
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

Usage pySpark
+++++++++++++

.. code-block:: python

	val url = "jdbc:postgresql://somehost:someport/somedb?user=someuser&currentSchema=someschema"
    pg = sc._jvm.fr.aphp.eds.spark.postgres.PGUtil.apply(spark._jsparkSession, url, "/tmp/")
    pg.inputBulk("select * from test2",False, 1, 1, "col").show()
    pg.purgeTmp()



Features
++++++++

- input
- inputBulk
- output
- ouputBulk
- outputScd1
- outputScd2
- tableTruncate
- tableDrop
- tableCopy
- tableMove
- sqlExec

Benchmark
+++++++++

Input
******
TODO

Output
******
TODO
