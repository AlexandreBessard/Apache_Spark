  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


  object Test12 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()

      // Sample data
      val data = 1 to 100

      // Create a DataFrame with 4 partitions
      import spark.implicits._
      val df: DataFrame = spark.sparkContext.parallelize(data, 4).toDF("value")

      // Function to show content of each partition
      def showPartitionIndex(idx: Int, iter: Iterator[(Int)]): Iterator[String] = {
        val cache = iter.toList
        val content = if (cache.isEmpty) "No data" else cache.mkString(", ")
        Iterator(s"Partition: $idx, Content: [$content]")
      }

      // Show content of each partition
      df.rdd.mapPartitionsWithIndex {
        (idx, iter) => showPartitionIndex(idx, iter.map(x => x.getInt(0)))
      }.collect().foreach(println)

      // Repartition the DataFrame into 8 partitions
      val repartitionedDF: DataFrame = df.repartition(8)

      // Get the number of partitions in the repartitioned DataFrame
      val numPartitions = repartitionedDF.rdd.partitions.length

      // Display the number of partitions
      println(s"Number of partitions in repartitionedDF: $numPartitions")

      // Specify the number partition you want to show
      val partitionNumberToShow = 1  // Replace with the desired partition number

      // Get the RDD of the DataFrame
      val rdd = repartitionedDF.rdd

      println("------> ")
      // Use foreachPartition to print data from all partitions
      rdd.foreachPartition { partition =>
        val currentPartitionIndex = org.apache.spark.TaskContext.get.partitionId()
        println(s"\nData from Partition $currentPartitionIndex:")
        partition.foreach(row => println(row.mkString("\t")))
      }

      // Stop the SparkSession
      spark.stop()

    }
  }