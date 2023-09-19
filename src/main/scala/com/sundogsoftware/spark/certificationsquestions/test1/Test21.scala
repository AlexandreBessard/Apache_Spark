package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_unixtime}

object Test21 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Shows the unix epoch timestamps in column transactionDate as String in the format
    month/day/year in column transactionDateFormatted
     */

    // Sample data for transactionsDf
    val data = Seq(
      (1, "A", 1632844800), // Timestamp: 09/28/2021
      (2, "B", 1632931200) // Timestamp: 09/29/2021
    )

    // Define the schema
    val schema = Seq(
      "transactionId", "productId", "transactionDate"
    )

    // Create a DataFrame
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Display the original DataFrame
    println("Original DataFrame:")
    transactionsDf.show()

    // Add a new column "transactionDateFormatted" with the formatted date
    val formattedDf = transactionsDf.withColumn(
      "transactionDateFormatted",
      from_unixtime(col("transactionDate"), "MM/dd/yyyy")
    )

    // Display the DataFrame with the formatted date column
    println("DataFrame with formatted date column:")
    formattedDf.show()

    // Stop the SparkSession
    spark.stop()
  }

}
