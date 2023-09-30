package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test3 {

  Logger.getLogger("org").setLevel(Level.ERROR)


  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data (replace this with your actual DataFrame)
    val data = Seq(
      (1, "ProductA", 10.5),
      (2, "ProductB", 15.2),
      (3, "ProductA", 12.8),
      (4, "ProductC", 8.3),
      (5, "ProductB", 9.7)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "itemName", "value")

    // Create a DataFrame from the sample data
    val itemDF = spark.createDataFrame(data).toDF(schema: _*)

    // Cache the DataFrame into memory and count the number of rows
    // Default storage is MEMORY_AND_DISK
    val rowCount = itemDF.cache().count()

    // Show the count
    println(s"Number of rows in the DataFrame: $rowCount")

    // Stop the SparkSession
    spark.stop()
  }

}
