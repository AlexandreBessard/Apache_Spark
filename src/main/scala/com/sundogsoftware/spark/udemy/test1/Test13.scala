package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, from_unixtime, unix_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Test13 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample DataFrame (you can replace this with your actual DataFrame)
    val transactionsDf: DataFrame = spark.createDataFrame(Seq(
      (1, "2023-09-26 10:15:00"),
      (2, "2023-09-27 15:30:00"),
      (3, "2023-09-28 08:45:00")
    )).toDF("transactionId", "transactionDate")

    // Add a new column "transactionDateFormatted" with formatted dates
    val formattedDf = transactionsDf.withColumn(
      "transactionDateFormatted",
      from_unixtime(unix_timestamp(col("transactionDate"), "yyyy-MM-dd HH:mm:ss"), "MM/dd/yyyy")
    )

    // Show the resulting DataFrame
    formattedDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
