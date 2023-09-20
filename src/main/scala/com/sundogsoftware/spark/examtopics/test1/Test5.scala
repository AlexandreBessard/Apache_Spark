package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object Test5 {

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
    val storesDF: DataFrame = Seq(
      ("Store A", "Location X", 10000, 35),
      ("Store B", "Location Y", 30000, 25),
      ("Store C", "Location Z", 20000, 40)
    ).toDF("StoreName", "Location", "sqft", "customerSatisfaction")

    // Use the filter method to filter rows based on multiple conditions
    val filteredStoresDF = storesDF
      .filter((col("sqft") <= 25000) || (col("customerSatisfaction") >= 30))

    // Show the resulting DataFrame
    filteredStoresDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
