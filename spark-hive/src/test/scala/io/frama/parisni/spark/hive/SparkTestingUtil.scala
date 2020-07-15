package io.frama.parisni.spark.hive

import org.apache.spark.sql.SparkSession

trait SparkTestingUtil {
  implicit lazy val spark = SparkSession
    .builder()
    .master("local[2]")
    .appName("spark session")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

}
