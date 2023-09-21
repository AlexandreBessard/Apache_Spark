package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Test21 {

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
      ("Store E", "Division 2", 28000)
    ).toDF("StoreName", "Division", "Sqft")

    // Define a custom function to assess performance for a row
    def assessPerformance(row: Row): String = {
      val storeName = row.getString(0)
      val division = row.getString(1)
      val sqft = row.getInt(2)
      // Perform performance assessment logic here
      val assessment = if (sqft > 25000) "High Performance" else "Low Performance"
      // String interpolator
      s"$storeName ($division): $assessment"
    }

    // Apply the assessPerformance function to each row in storesDF using collect()
    val assessments = storesDF.collect().map(assessPerformance)

    // Print the assessments
    assessments.foreach(println)

    // Stop the SparkSession
    spark.stop()
  }

}
