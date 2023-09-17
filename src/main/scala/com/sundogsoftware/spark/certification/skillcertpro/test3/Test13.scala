  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


  object Test13 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()

      // Sample data
      val data = 1 to 100

      // Create a DataFrame with 16 partitions
      import spark.implicits._
      val df: DataFrame = spark.sparkContext.parallelize(data, 16).toDF("value")

      // Coalesce the DataFrame into 8 partitions
      /*
      We use the coalesce(8) method to reduce the number of partitions in the DataFrame df to 8.
       */
      val coalescedDF: DataFrame = df.coalesce(8)

      // Get the number of partitions in the coalesced DataFrame
      val numPartitions = coalescedDF.rdd.partitions.length

      // Display the number of partitions
      println(s"Number of partitions in coalescedDF: $numPartitions")

      // Stop the SparkSession
      spark.stop()

    }
  }