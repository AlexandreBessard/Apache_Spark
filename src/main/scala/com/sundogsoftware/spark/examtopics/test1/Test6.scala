package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
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
    Return a new DataFrame where the column is the type string.
     */

    // Import implicits for DataFrame operations
    import spark.implicits._

    // Create a DataFrame with sample data
    val storesDF: DataFrame = Seq(
      (1, "Store A", "Location X", 100000),
      (2, "Store B", "Location Y", 150000),
      (3, "Store C", "Location Z", 80000)
    ).toDF("StoreID", "StoreName", "Location", "Revenue")

    val storesDF1: DataFrame = Seq(
      (1, "Store A", "Location X", 100000),
      (2, "Store B", "Location Y", 150000),
      (3, "Store C", "Location Z", 80000)
    ).toDF

    // Colum named by default
    storesDF1.show()

    // Add a new column "storeId" by casting the "StoreID" column to StringType
    val storesWithNewColumnDF =
      storesDF.withColumn("storeId", col("StoreID").cast(StringType))

    // Show the resulting DataFrame
    storesWithNewColumnDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
