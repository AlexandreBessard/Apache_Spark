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
      val df: DataFrame = spark
        .sparkContext.parallelize(data, 16).toDF("value")

      // Display content of original DataFrame partitions
      println("\nData in original DataFrame (16 partitions):")
      showPartitionData(df)

      // Coalesce the DataFrame into 8 partitions
      /*
      We use the coalesce(8) method to reduce the number of partitions in the DataFrame df to 8.
       */
      // The data from partition 16 to 8 has been changed
      val coalescedDF: DataFrame = df.coalesce(8)

      // Display content of coalesced DataFrame partitions
      println("\nData in coalesced DataFrame (8 partitions):")
      showPartitionData(coalescedDF)

      // Get the number of partitions in the coalesced DataFrame
      val numPartitions = coalescedDF.rdd.partitions.length

      // Display the number of partitions
      println(s"Number of partitions in coalescedDF: $numPartitions")

      // Stop the SparkSession
      spark.stop()

    }

    def showPartitionData(df: DataFrame): Unit = {
      df.rdd.foreachPartition { partition =>
        val partitionIndex = org.apache.spark.TaskContext.get.partitionId()
        val partitionData = partition.map(_.mkString("\t")).toList
        if (partitionData.nonEmpty) {
          println(s"\nData from Partition $partitionIndex:")
          partitionData.foreach(println)
        } else {
          println(s"\nPartition $partitionIndex is empty.")
        }
      }
    }
  }