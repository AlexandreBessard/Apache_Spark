package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test4 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    //See different syntax

    // Import implicits for DataFrame operations
    import spark.implicits._

    // Create a DataFrame with sample data
    val data = Seq(
      ("Alice", 25),
      ("Bob", 30),
      ("Charlie", 28),
      ("David", 22),
      ("Eve", 35)
    ).toDF("Name", "Age")

    // Using the filter method
    val filteredDataFilter = data.filter($"Age" > 25)

    // Use the filter method to filter rows where "sqft" is less than or equal to 25000
    val filteredStoresDF1 = data.filter(col("Age") > 25)

    // Using the where method
    val filteredDataWhere = data.where($"Age" > 25)

    println("Other same result")
    filteredStoresDF1.show()

    // Show the filtered DataFrames
    println("Using filter method:")
    filteredDataFilter.show()

    println("Using where method:")
    filteredDataWhere.show()

    // Stop the SparkSession
    spark.stop()
  }

}
