package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test34 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("FilterAndLimitExample")
      .master("local[*]")
      .getOrCreate()

    // Sample DataFrame (Replace with your actual DataFrame)
    val data = Seq(
      (1, "Item A", 10.0),
      (2, "Item B", 15.0),
      (3, "Item C", 12.0),
      (4, "Item D", 8.0),
      (5, "Item E", 18.0)
    )

    val schema = List("itemId", "itemName", "value")
    val itemsDf: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Define the output file path
    val filePath = "output.parquet"

    // Write the DataFrame to Parquet format with "overwrite" mode
    itemsDf.write
      .mode("overwrite")
      .parquet(filePath)

    // Stop the SparkSession
    spark.stop()
  }
}
