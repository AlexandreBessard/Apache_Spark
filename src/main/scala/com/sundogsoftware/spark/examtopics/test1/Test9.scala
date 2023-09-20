package com.sundogsoftware.spark.examtopics.test1

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Test9 {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    New DataFrame split at the underscore character into columns
     */

    // Import implicits for DataFrame operations
    import spark.implicits._

    // Create a DataFrame with sample data
    val storesDF: DataFrame = Seq(
      ("Store A", "High_Value_Large"),
      ("Store B", "Low_Value_Small"),
      ("Store C", "Medium_Value_Medium")
    ).toDF("StoreName", "storeCategory")

    // Split the "storeCategory" column and create new columns
    val splitDF = storesDF
      .withColumn("storeValueCategory", split(col("storeCategory"), "_")(0))
      .withColumn("storeSizeCategory", split(col("storeCategory"), "_")(2))

    // Show the resulting DataFrame
    splitDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
