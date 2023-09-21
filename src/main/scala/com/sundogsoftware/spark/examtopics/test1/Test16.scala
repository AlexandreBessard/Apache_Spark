package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{approx_count_distinct, col}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test16 {

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
    val data = Seq(
      ("Alice", 25),
      ("Bob", 30),
      ("David", 22),
      ("Eve", 35)
    ).toDF("Name", "Age")

    // Get the number of rows in the DataFrame
    val rowCount = data.count()

    // Print the number of rows
    println(s"Number of Rows: $rowCount")



    // Stop the SparkSession
    spark.stop()
  }

}
