package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test35 {
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
      (3, "Item C", 12.0)
    )

    val schema = List("itemId", "itemName", "value")
    val transactionsDf: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Print the schema of the DataFrame
    transactionsDf.select("value").printSchema()

    // Stop the SparkSession
    spark.stop()
  }
}
