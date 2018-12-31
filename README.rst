SPARK-POSTGRES
==============

spark-postgres is a set of function to better bridge postgres and spark. It
focuses on stability and speed in ETL workloads.

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
