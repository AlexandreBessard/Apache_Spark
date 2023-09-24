package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StructField, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test34 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Define the file path where the JSON data is located
    val filePath = "/path/to/json/file.json"

    // Define a schema for the JSON data
    val schema = new StructType()
      .add("Name", StringType)
      .add("Age", IntegerType)
      .add("City", StringType)

    // Read the JSON data into a DataFrame with the specified schema
    val storesDF: DataFrame = spark.read
      .schema(schema)
      .json(filePath)

    // Show the content of the DataFrame
    storesDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
