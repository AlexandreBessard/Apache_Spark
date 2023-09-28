package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Test19 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val data = Seq(
      (1, "Apple Orange Banana"),
      (2, "Grapes Cherry"),
      (3, "Lemon Lime"),
      (4, "Pear Kiwi Strawberry")
    )

    // Define the schema for the DataFrame
    val schema = List("itemId", "itemName")

    // Create a DataFrame from the sample data
    val itemsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Split the "itemName" column into elements
    val itemsWithSplitDf = itemsDf.withColumn("itemNameElements",
      split(col("itemName"), " "))

    // Filter rows based on the size of "itemNameElements" column
    val filteredItemsDf = itemsWithSplitDf
      .filter(size(col("itemNameElements")) >= 3)

    // Show the filtered DataFrame
    filteredItemsDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
