SPARK-POSTGRES
==============

spark-postgres is a set of function to better bridge postgres and spark. It
focuses on stability and speed in ETL workloads.

Usage
*****

.. code-block:: scala
	
	import fr.aphp.eds.spark.postgres.PGUtil
	// the connection looks into /home/$USER/.pgpass for a password
	val url = ""jdbc:postgresql://somehost:someport/somedb?user=someuser&currentSchema=someschema"
	// get a dataframe from a PGCOPY stmt
	val df = PGUtil.inputQueryBulkDf(spark, url, "select * from sometable", "/tmp/export.csv", false)
	// bulk load a temporary table and apply a SCD1 on sometable from the previous df with a batch size of 50k rows
	PGUtil.outputBulkDfScd1(url, "sometable", "somekey", df, 50000)



Input from Postgres
*******************
- inputQueryDf
- inputQueryBulkDf
- inputQueryBulkCsv

Ouput to Postgres
*****************
- outputBulk

Slow Changing Dimension
***********************
- outputBulkDfScd1
- outputBulkDfScd2


Table functions
***************
- tableCreate
- tableCopy
- tableTruncate
- tableDrop

General functions
*****************
- connOpen


