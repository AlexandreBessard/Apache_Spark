package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Test22 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data (replace this with your actual DataFrame)
    val data = Seq(
      (1, 0.1, "Value1"),
      (2, 0.2, "Value2"),
      (3, 0.3, "Value3"),
      (4, 0.4, "Value4"),
      (5, 0.5, "Value5")
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "predError", "value")

    // Create a DataFrame from the sample data
    val transactionDf = spark.createDataFrame(data).toDF(schema: _*)

    // Filter rows where "transactionId" is even and select specific columns
    val filteredAndSelectedDf = transactionDf
      .filter(col("transactionId") % 2 === 0)
      .select("predError", "value")

    // Show the filtered and selected DataFrame
    filteredAndSelectedDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
