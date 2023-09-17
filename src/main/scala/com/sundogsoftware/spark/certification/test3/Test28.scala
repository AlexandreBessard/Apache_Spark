  package com.sundogsoftware.spark.certification.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel


  object Test28 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()


      // Sample data and DataFrame creation
      val a = Array(1002, 3001, 4002, 2003, 2002, 3004, 1003, 4006)
      val seqA: Seq[Int] = a.toSeq
      println("----> " + seqA)

      // Create a DataFrame from the Seq with a constant column "x"

      val df = spark.createDataFrame(seqA.map(value => (
        value, //Represents the first column named "value"
        value % 1000))) // Represents the second colum named "x"
        .toDF("value", "x")

      // Cache the DataFrame fully in memory and on disk
      df.persist(StorageLevel.MEMORY_AND_DISK)

      // Check if the DataFrame is cached by examining its storage level
      val isCached = df.storageLevel != StorageLevel.NONE
      println("Is DataFrame cached? " + isCached)

      // Perform operations on the cached DataFrame
      df.show()

      // Stop the SparkSession
      spark.stop()

    }
  }
