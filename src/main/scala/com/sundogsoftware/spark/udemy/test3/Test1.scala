package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test1 {

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test29")
      .master("local[*]")
      .getOrCreate()


    // Sample data for today's and yesterday's transactions
    val todayData = Seq(
      (1, "ProductA", 10.0),
      (2, "ProductB", 15.0),
      (3, "ProductC", 20.0)
    )

    val yesterdayData = Seq(
      (4, "ProductD", 25.0),
      (5, "ProductE", 30.0)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "itemName", "amount")

    val schema1 = List("test", "test", "test")


    // Create DataFrames for today and yesterday
    val todayTransactionsDf = spark.createDataFrame(todayData).toDF(schema: _*)
    val yesterdayTransactionsDf = spark.createDataFrame(yesterdayData).toDF(schema1: _*)

    // Union the two DataFrames
    // Columns from schema1 will be override by DataFrame: todayTransactionsDf
    val combinedTransactionsDf: DataFrame = todayTransactionsDf.union(yesterdayTransactionsDf)

    // Show the resulting DataFrame
    combinedTransactionsDf.show()


    // Stop the SparkSession
    spark.stop()
  }
}
