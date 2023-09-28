package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test4 {

  Logger.getLogger("org").setLevel(Level.ERROR)


  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Read the JSON file and create a DataFrame
    val jsonFilePath = "path/to/people.json" // Replace with the actual path to your JSON file
    val peopleDF: DataFrame = spark.read.json(jsonFilePath)

    // Show the content of the DataFrame
    peopleDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
