package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test11 {

  Logger.getLogger("org").setLevel(Level.ERROR)


  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Import implicits for DataFrame operations
    import spark.implicits._

    /*
    Return new DataFrame by removing "Description:"
     */

    // Create a DataFrame with sample data
    val storesDF: DataFrame = Seq(
      ("Store A", "Description: Premium Electronics Store"),
      ("Store B", "Description: Books and Stationery"),
      ("Store C", "Description: Clothing Boutique")
    ).toDF("StoreName", "storeDescription")

    // Use the regexp_replace function to remove the prefix "Description: " from store descriptions
    val modifiedDF = storesDF
      .withColumn("storeDescription", regexp_replace(col("storeDescription"), "^Description: ", ""))

    // Show the resulting DataFrame
    modifiedDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
