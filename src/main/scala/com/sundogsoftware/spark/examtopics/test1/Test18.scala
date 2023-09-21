package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test18 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Import implicits for DataFrame operations
    import spark.implicits._

    // Create a DataFrame with sample store data
    val storesDF: DataFrame = Seq(
      ("Store A", 25000, 30),
      ("Store B", 30000, 25),
      ("Store C", 20000, 40),
      ("Store D", 35000, 35),
      ("Store E", 28000, 28)
    ).toDF("StoreName", "Sqft", "Employees")

    // Use the describe() method to generate summary statistics for numerical columns
    val summaryDF = storesDF.describe()

    // Show the resulting summary DataFrame
    summaryDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
