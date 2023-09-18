package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col, explode}

object Test12 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    The requirement is to create a DataFrame that contains two columns, "itemId" and "col."
    In this new DataFrame, for each element in the "attributes" column of DataFrame "itemDf,"
    there should be a separate row. The "itemId" column should contain the associated itemId from DataFrame "itemsDf."
    The new DataFrame should only include rows from DataFrame "itemsDf" where the "attributes" column contains the element "cozy."
     */

    // Sample data for itemsDf
    val itemsData = Seq(
      (1, Seq("cozy", "soft")),
      (2, Seq("warm", "cozy")),
      (3, Seq("cozy", "soft")),
      (4, Seq("comfortable"))
    )

    // Define the schema
    val itemsSchema = Seq(
      "itemId", "attributes"
    )

    // Create a DataFrame
    val itemsDf = spark.createDataFrame(itemsData).toDF(itemsSchema: _*)

    // Display the original DataFrame
    println("Original DataFrame:")
    itemsDf.show()

    // Step 1: Filter rows where "attributes" contains "cozy"
    val filteredDf = itemsDf.filter(array_contains(col("attributes"), "cozy"))

    // Display the filtered DataFrame
    println("Filtered DataFrame:")
    filteredDf.show()

    // Step 2: Select "itemId" and explode "attributes"
    val resultDf = filteredDf
      .select(col("itemId"), explode(col("attributes")).as("col"))

    // Display the final DataFrame
    println("Final DataFrame:")
    resultDf.show()

    // Stop the SparkSession
    spark.stop()
  }

}
