package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Test22 {

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
      ("Store A", "Division 2", 25000),
      ("Store B", "Division 1", 30000),
      ("Store C", "Division 3", 20000),
      ("Store D", "Division 1", 35000),
      ("Store E", "Division 2", 28000) // By default the Integer column is set to nullable = false
    ).toDF("StoreName", "Division", "Sqft")

    // Print the schema of the DataFrame
    storesDF.printSchema()

    // Stop the SparkSession
    spark.stop()
  }

}
