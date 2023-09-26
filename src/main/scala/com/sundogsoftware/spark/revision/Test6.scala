package com.sundogsoftware.spark.revision

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test6 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Compare where and filter methods and we have the same result.
    where and filter are the same but they take different parameter type.
     */

    // Sample DataFrame with a "predError" column
    val data = Seq((1, 2.5), (2, 3.0), (3, 6.0), (4, 1.5), (5, 5.5))
    val schema = List("id", "predError")
    val transactionsDF: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Using where to filter rows where "predError" is greater than or equal to 5
    val filteredDF1 = transactionsDF.where("predError >= 5")
    // Show the resulting DataFrame
    filteredDF1.show()

    // Using filter to filter rows where "predError" is greater than or equal to 5
    val filteredDF2 = transactionsDF.filter(col("predError") >= 5)
    // Show the resulting DataFrame
    filteredDF2.show()

    // Stop the SparkSession
    spark.stop()
  }
}
