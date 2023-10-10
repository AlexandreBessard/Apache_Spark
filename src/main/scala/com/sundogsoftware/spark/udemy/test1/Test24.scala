package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Test24 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Which of the following code blocks returns a one-column DataFrame of
    all values in column supplier of DataFrame itemsDf that do not contain
    the letter X? In the DataFrame, every value should only be listed once.
     */

    // Sample data
    val data = Seq(
      (1, "Thick Coat for Winter", Array("blue", "winter", "cozy"), "Sports Company Inc."),
      (2, "Elegant Outdoors Jacket", Array("red", "summer", "fresh"), "YetiX"),
      (3, "Outdoors Backpack", Array("green", "summer", "travel"), "Sports Company Inc.")
    )

    // Define the schema for the DataFrame
    val schema = List("itemId", "itemName", "attributes", "supplier")

    // Create a DataFrame from the sample data
    val itemsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Filter and select distinct values in "supplier" column
    val filteredSupplierDf = itemsDf
      .filter(!col("supplier").contains("X"))
      .select("supplier")
      .distinct() // unique result, it is NOT a action function

    // Show the DataFrame with unique supplier values
    filteredSupplierDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
