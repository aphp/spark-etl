package fr.aphp.wind.eds.omop.orbis

import org.apache.spark.sql.{QueryTest, SparkSession}


trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }

}
