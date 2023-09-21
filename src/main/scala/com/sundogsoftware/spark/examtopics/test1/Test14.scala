package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{coalesce, col, lit}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Test14 {

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
    Drop duplicate rows
     */

    // Create a DataFrame with sample data, including duplicate rows
    val data = Seq(
      ("Alice", 25, "New York"),
      ("Bob", 30, "San Francisco"),
      ("Alice", 25, "New York"), // Duplicate
      ("David", 22, "Los Angeles"),
      ("Eve", 35, "Chicago"),
      ("Bob", 30, "San Francisco") // Duplicate
    ).toDF("Name", "Age", "Location")

    // Remove duplicate rows based on all columns
    val noDuplicatesDF = data.dropDuplicates()

    // Show the resulting DataFrame with no duplicate rows
    noDuplicatesDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
