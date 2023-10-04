package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test5 {

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test3")
      .master("local[*]")
      .getOrCreate()

    // Sample data (Replace with your actual data or file path)
    val data = Seq(
      (1, "ProductA"),
      (2, "ProductB"),
      (3, "ProductC"),
      (4, "ProductD"),
      (5, "ProductE")
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "itemName")

    // Create a DataFrame from the sample data
    val transactionsDf: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Get the current number of partitions
    val currentPartitions = transactionsDf.rdd.getNumPartitions

    // Repartition the DataFrame to increase the number of partitions by 2
    val repartitionedDf = transactionsDf.repartition(currentPartitions + 2)

    // Show the number of partitions before and after repartitioning
    println(s"Number of partitions before repartitioning: $currentPartitions")
    println(s"Number of partitions after repartitioning: ${repartitionedDf.rdd.getNumPartitions}")

    // Stop the SparkSession
    spark.stop()
  }
}
