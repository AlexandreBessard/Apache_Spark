package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test3 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Create a new DateFrame with a subset of columns specified by name.
     */

    // Import implicits for DataFrame operations
    import spark.implicits._

    // Create a DataFrame (assuming you have storesDF)
    val storesDF: DataFrame = Seq(
      (1, "Store A", "Location X", 100000),
      (2, "Store B", "Location Y", 150000),
      (3, "Store C", "Location Z", 80000)
    ).toDF("StoreID", "StoreName", "Location", "Revenue")

    // Define a list of column names you want to select
    val selectedColumns = Seq("StoreName", "Revenue")

    // Use the select operation to create a new DataFrame with the selected columns
    val selectedDF = storesDF.select(selectedColumns.map(col): _*)

    // Show the resulting DataFrame
    selectedDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
