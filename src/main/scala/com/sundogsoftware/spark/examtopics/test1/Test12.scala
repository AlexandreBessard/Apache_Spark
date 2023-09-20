package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test12 {

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
    Renamed multiple columns
     */

    // Create a DataFrame with sample data
    val storesDF: DataFrame = Seq(
      ("Store A", "New York", "John Doe"),
      ("Store B", "California", "Jane Smith"),
      ("Store C", "Texas", "Robert Johnson")
    ).toDF("StoreName", "division", "managerName")

    // Rename columns "division" to "state" and "managerName" to "managerFullName"
    val renamedDF = storesDF
      .withColumnRenamed("division", "state")
      .withColumnRenamed("managerName", "managerFullName")

    // Show the resulting DataFrame
    renamedDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
