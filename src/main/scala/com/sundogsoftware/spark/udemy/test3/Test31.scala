package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, corr, udf}

object Test31 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("TestUDF")
      .master("local[*]")
      .getOrCreate()

    // Sample data (Replace with your actual data or DataFrame)
    val data = Seq(
      (1, 3.0, 5.0),
      (2, 2.5, 4.5),
      (3, 4.0, 5.5),
      (4, 5.0, 6.0),
      (5, 3.5, 5.2)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "predError", "value")

    // Create a DataFrame from the sample data
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Calculate the correlation between "predError" and "value" columns and alias it as "corr"
    val resultDf = transactionsDf
      .select(corr(col("predError"), col("value")).alias("corr"))

    // Show the resulting DataFrame
    resultDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
