package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{approx_count_distinct, col}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test15 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Import implicits for DataFrame operations
    import spark.implicits._



    // Create a DataFrame with sample data
    val storesDF: DataFrame = Seq(
      ("Store A", "New York"),
      ("Store B", "California"),
      ("Store C", "Texas"),
      ("Store D", "New York"),
      ("Store E", "California")
    ).toDF("StoreName", "division")

    // Calculate the approximate count of distinct values in the "division" column
    // with a relative error of 0.15 and alias the result as "divisionDistinct"
    /*
    The higher the relative error parameter, the less accurate and faster.
    The lower the relative error parameter, the more accurate and slower.
     */
    val distinctCountDF = storesDF
      .agg(approx_count_distinct(col("division"), 0.15).alias("divisionDistinct"))

    // Show the resulting DataFrame
    distinctCountDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
