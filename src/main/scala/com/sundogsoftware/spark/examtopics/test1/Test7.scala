package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test7 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Return new DataFrame by adding a new column to it
     */

    // Import implicits for DataFrame operations
    import spark.implicits._

    // Create a DataFrame with sample data
    val storesDF: DataFrame = Seq(
      ("Store A", 10000, 5),
      ("Store B", 30000, 10),
      ("Store C", 20000, 8)
    ).toDF("StoreName", "sqft", "numberOfEmployees")

    // Add a new column "employeesPerSqft" by calculating employees per square foot
    val storesWithNewColumnDF =
      storesDF.withColumn("employeesPerSqft", col("numberOfEmployees") / col("sqft"))

    // Show the resulting DataFrame
    storesWithNewColumnDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
