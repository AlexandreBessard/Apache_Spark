package com.sundogsoftware.spark.examtopics.test1

import breeze.linalg.sum
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test17 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Import implicits for DataFrame operations
    import spark.implicits._

    // Create a DataFrame with sample sales data
    val salesData: DataFrame = Seq(
      ("Product A", "Category 1", 100.0),
      ("Product B", "Category 2", 150.0),
      ("Product C", "Category 1", 200.0),
      ("Product D", "Category 2", 120.0),
      ("Product E", "Category 1", 180.0)
    ).toDF("Product", "Category", "Revenue")

    // Group the data by "Category" and calculate the total revenue for each category
    val groupedDF = salesData
      .groupBy("Category").avg("Revenue")

    // Show the resulting DataFrame
    groupedDF.show()
    // Stop the SparkSession
    spark.stop()
  }

}
