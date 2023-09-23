package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object Test25 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Create a DataFrame with sample data
    val data = spark.range(1, 1000000)

    // Using cache() with default storage level (MEMORY_AND_DISK)
    data.cache()

    // Using persist() with custom storage level (DISK_ONLY)
    data.persist(StorageLevel.DISK_ONLY)

    // Unpersist the data (both cache() and persist() can be unpersisted)
    data.unpersist()

    // Stop the SparkSession
    spark.stop()
  }

}
