package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test32 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Import implicits for DataFrame operations
    import spark.implicits._

    // Sample data for DataFrame "storesDF"
    val data: Seq[(Int, String, String)] = Seq(
      (1, "Store A", "East"),
      (2, "Store B", "West"),
      (3, "Store C", "East"),
      (4, "Store D", "West")
    )
    val storesDF: DataFrame = data.toDF("StoreId", "StoreName", "Division")

    // Define the file path for writing Parquet data
    val filePath = "/path/to/output/parquet"

    // Write DataFrame to Parquet and partition by "Division" column
    storesDF.write
      .partitionBy("Division")
      .parquet(filePath)

    // Stop the SparkSession
    spark.stop()
  }

}
