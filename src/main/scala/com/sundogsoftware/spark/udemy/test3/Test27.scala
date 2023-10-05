package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{cos, degrees, round}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test27 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("ItemNameSeparatorExample")
      .master("local[*]")
      .getOrCreate()

    /*
    The code block shown below should return a copy of DataFrame transactionsDf with an
    added column cos. This column should have the values in column value converted to
    degrees and having the cosine of those converted values taken, rounded to two decimals.
    Choose the answer that correctly fills the blanks in the code block to accomplish this.
     */

    // Sample data for transactionsDf
    val data = Seq(
      (1, 10.0),
      (2, 15.0),
      (3, 12.0),
      (4, 8.0),
      (5, 18.0)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "value")

    // Create a DataFrame from sample data
    val transactionsDf: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Calculate the cosine of the "value" column and round it to two decimal places
    val resultDf = transactionsDf
      .withColumn("cos", round(cos(degrees(transactionsDf("value"))), 2))

    // Show the resulting DataFrame
    resultDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
