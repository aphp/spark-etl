package io.frama.parisni.spark.meta

import org.apache.spark.sql.QueryTest

class MetaSyncTest extends QueryTest
  with SparkSessionTestWrapper {


  test("test loading postgres") {

    val yaml = getClass.getResource("/meta/config.yaml").getPath
    println(yaml)

    // MetaSync.run(spark, yaml)
  }
}
