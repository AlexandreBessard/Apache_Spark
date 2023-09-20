package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_unixtime}

object Test22 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data for transactionsDf
    val data = Seq(
      (1, "A", 1632844800), // Timestamp: 09/28/2021
      (2, "B", 1632931200), // Timestamp: 09/29/2021
      (3, "A", 1632844800),
      (4, "C", 1632931200),
      (5, "B", 1632844800),
      (6, "A", 1632931200),
      (7, "C", 1632844800),
      (8, "A", 1632931200)
    )

    // Define the schema
    val schema = Seq(
      "transactionId", "productId", "transactionDate"
    )

    // Create a DataFrame
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Display the original number of partitions
    println(s"Original number of partitions: ${transactionsDf.rdd.getNumPartitions}")

    // Repartition the DataFrame into 14 partitions based on "storeId" and "transactionDate"
    val repartitionedDf = transactionsDf
      .repartition(14, col("transactionId"), col("transactionDate"))

    // Count the number of rows in each partition
    val partitionCounts =
      repartitionedDf.rdd.mapPartitions(iter => Iterator(iter.length)).collect()

    // Display the count of rows in each partition
    // 12 partitions, we have more partition than individual data so few partition will have 0 row.
    partitionCounts.zipWithIndex.foreach { case (count, index) =>
      println(s"Partition $index has $count rows")
    }

    // Stop the SparkSession
    spark.stop()
  }

}
