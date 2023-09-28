package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object Test25 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val data = Seq(
      (1, "ProductA", Array("blue", "winter", "cozy"), "Sports Company Inc."),
      (2, "ProductB", Array("red", "summer", "fresh"), "YetiX"),
      (3, "ProductC", Array("green", "summer", "travel"), "Sports Company Inc.")
    )

    // Define the schema for the DataFrame
    val schema = List("itemId", "itemName", "attributes", "supplier")

    // Create a DataFrame from the sample data
    val itemsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Register the DataFrame as a temporary view
    itemsDf.createOrReplaceTempView("items")

    // Execute a SQL query using spark.sql
    val resultDf = spark
      .sql("SELECT supplier, attribute FROM items LATERAL VIEW explode(attributes) AS attribute")

    // Show the result DataFrame
    resultDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
