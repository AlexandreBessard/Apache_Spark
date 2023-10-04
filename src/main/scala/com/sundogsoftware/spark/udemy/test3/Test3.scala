package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test3 {

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
      (1, 100.0),
      (2, 200.0),
      (3, 300.0),
      (4, 400.0)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "value")

    // Create a DataFrame from the sample data
    val transactionsDf: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Select 'value' column
    val valueDf = transactionsDf.select("value")

    valueDf.show()

    // Create a DataFrame with a 'productId' column (as there is none in the sample data)
    val productIdDf = transactionsDf.selectExpr("transactionId as productId")

    productIdDf.show()

    // Perform a union of 'valueDf' and 'productIdDf'
    val unionDf = valueDf.union(productIdDf).distinct()

    // Show the resulting DataFrame
    unionDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
