package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Test16 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val data = Seq(
      (1, 20, 2),
      (1, 20, 4),
      (2, 25, 2),
      (3, 30, 3),
      (4, 15, 2),
      (5, 22, 1)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "storeId", "productId")

    // Create a DataFrame from the sample data
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Apply the filter condition
    val filteredDf = transactionsDf
      .filter((col("storeId").between(20, 30))
        // && means both conditions have to be met.
        && (col("productId") === 2))

    // Show the filtered DataFrame
    filteredDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
