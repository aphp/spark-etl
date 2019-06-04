SPARK-CSV
=========

spark-csv is an extension to the official spark csv reader. This tool allows to
define a data schema, with mandatory columns and optional ones with default
values.

Guaranties
==========

The process verifies:

- types
- nullable
- required fields
- column order (whenever the input csv is not well ordered)

Prerequirements
===============

- the csv SHALL have a header
- the schema SHALL be defined carefully
- the default values SHALL be defined as Metadata String or Null (later they are casted)
- the csv SHALL be well formed otherwize it will failfast

Use the lib
+++++++++++

To compile the code, clone it and use maven to build the shaded jar into the target folder.

- `mvn install`

Usage Scala
+++++++++++

.. code-block:: scala
	
	import io.frama.parisni.csv.CSVTool
	import org.apache.spark.sql.types._

	// define default values
	val defaultValue = new MetadataBuilder().putString("default", "3.2").build
	val defaultComment = new MetadataBuilder().putNull("default").build

	// define the schema
	val schema = StructType(
	StructField("id", IntegerType, false) ::  // required
	StructField("code", StringType, false) :: // required
	StructField("value", DoubleType, false, defaultValue) :: // optional, default = 3.2
	StructField("comment", StringType, true, defaultComment) :: // optional, default = NULL
	Nil
	)

	// read the csv
	val df = CSVTool(spark, "file.csv", schema)
	df.show
	// +---+----+-----+-------+
	// | id|code|value|comment|
	// +---+----+-----+-------+
	// |  1| bob|  3.2|   null|
	// |  2| jim|  3.2|   null|
	// +---+----+-----+-------+
	
	df.printSchema
	// root
 	// |-- id: integer (nullable = false)
 	// |-- code: string (nullable = false)
 	// |-- value: double (nullable = false)
 	// |-- comment: string (nullable = true)

	df.schema.prettyJson
	// {
	//   "type" : "struct",
	//   "fields" : [ {
	//     "name" : "id",
	//     "type" : "integer",
	//     "nullable" : false,
	//     "metadata" : { }
	//   }, {
	//     "name" : "code",
	//     "type" : "string",
	//     "nullable" : false,
	//     "metadata" : { }
	//   }, {
	//     "name" : "value",
	//     "type" : "double",
	//     "nullable" : false,
	//     "metadata" : {
	//       "default" : "3.2"
	//     }
	//   }, {
	//     "name" : "comment",
	//     "type" : "string",
	//     "nullable" : true,
	//     "metadata" : {
	//       "default" : null
	//     }
	//   } ]
	// }


