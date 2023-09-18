package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

object Test3 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val data = Seq(
      (1, "A", 100, "2023-09-01"),
      (2, "B", 200, "2023-09-02"),
      (3, "C", 300, "2023-09-03")
    )

    // Define schema
    val schema = Seq(
      "transactionId", "itemId", "amount", "date"
    )

    // Create a DataFrame
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Display the original DataFrame
    println("Original DataFrame:")
    transactionsDf.show()

    // Persist the DataFrame in memory
    transactionsDf.persist(StorageLevel.MEMORY_ONLY)

    // Check if the DataFrame is cached by examining its storage level
    val isCached = transactionsDf.storageLevel != StorageLevel.NONE
    println("Is DataFrame cached? " + isCached)

    // Perform some operations on the cached DataFrame
    val resultDf = transactionsDf.filter(col("amount") > 100)
    println("Filtered DataFrame:")
    resultDf.show()

    // Unpersist the DataFrame from memory (optional)
    transactionsDf.unpersist()

    // Stop the SparkSession
    spark.stop()
  }
}
