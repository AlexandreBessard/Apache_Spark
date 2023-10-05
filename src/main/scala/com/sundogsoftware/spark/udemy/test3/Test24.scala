package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test24 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("ItemNameSeparatorExample")
      .master("local[*]")
      .getOrCreate()

    // Sample data
    val data = Seq(
      (1, "Alice", 25, 50000.0),
      (2, "Bob", 30, 60000.0),
      (3, "Charlie", 28, 75000.0),
      (4, "David", 35, 90000.0),
      (5, "Eve", 22, 55000.0)
    )

    // Define schema for the DataFrame
    val schema = List("id", "name", "age", "salary")

    // Create a DataFrame from sample data
    val df: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Use summary() to compute summary statistics
    df.summary().show()

    // Stop the SparkSession
    spark.stop()
  }
}
