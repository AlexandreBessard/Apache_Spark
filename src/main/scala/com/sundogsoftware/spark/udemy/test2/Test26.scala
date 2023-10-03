package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test26 {

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test21")
      .master("local[*]")
      .getOrCreate()

    // Sample data (replace this with your actual DataFrame)
    val data = Seq(
      (1, 100.0, 20.0, "A"),
      (2, 150.0, 30.0, "B"),
      (3, 200.0, 40.0, "C")
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "predError", "value", "f")

    // Create a DataFrame from the sample data
    val transactionsDf: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Select specific columns using the select method
    val selectedColumnsDf = transactionsDf.select("transactionId", "value", "f")

    // Show the resulting DataFrame
    selectedColumnsDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
