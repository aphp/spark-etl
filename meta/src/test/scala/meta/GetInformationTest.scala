package meta

import fr.aphp.wind.eds.omop.orbis.SparkSessionTestWrapper
import org.apache.spark.sql.QueryTest

class GetInformationTest extends QueryTest
  with SparkSessionTestWrapper
  with FeatureExtract {

/*
  test("test get postgres view") {

    val csv = getClass.getResource("/meta/spark-table.csv").getPath
    //assert(CSVTool.getCsvHeaders(spark, csv, None).mkString(",") === "lib_database,lib_schema,lib_table,typ_table,lib_column,typ_column,order_column")

    val pgView = CSVTool.read(spark, csv, delimiter = None, escape = None, multiline = None, dateFormat = None, timestampFormat = None).cache

    // extraire la source
    var res = extractSource(pgView)

    // extraire la pk
    res = extractPrimaryKey(res)

    // extraire les fk
    res = extractForeignKey(res)


    val database = generateDatabase(res).show
    val schema = generateSchema(res).show
    val table = generateTable(res).show
    val column = generateColumn(res).show

    val reference = inferForeignKey(res)
    reference.show

  }

 */
}


