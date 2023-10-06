package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Test40 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("FilterAndLimitExample")
      .master("local[*]")
      .getOrCreate()

    /*
      Return the number of unique values in column storeId of DataFrame transactionDf
     */

    // Sample data for transactionsDf
    val data = Seq(
      (1, "ProductA", 101, 1),
      (2, "ProductB", 102, 2),
      (3, "ProductA", 101, 3),
      (4, "ProductC", 104, 1),
      (5, "ProductB", 102, 2)
    )

    // Define the schema for transactionsDf
    val transactionSchema = List("transactionId", "productName", "productId", "storeId")

    // Create a DataFrame from the sample data
    val transactionsDf: DataFrame = spark.createDataFrame(data).toDF(transactionSchema: _*)

    // Select "storeId" column, drop duplicates, and count unique store IDs
    val uniqueStoreCount = transactionsDf.select("storeId").dropDuplicates().count()

    // Display the count of unique store IDs
    println(s"Number of unique store IDs: $uniqueStoreCount")

    // Stop the SparkSession
    spark.stop()
  }
}
