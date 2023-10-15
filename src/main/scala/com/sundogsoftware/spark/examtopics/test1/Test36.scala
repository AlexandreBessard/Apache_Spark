package com.sundogsoftware.spark.examtopics.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._

object Test36 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Define a schema
    val userSchema = StructType(Array(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("city", StringType, true)
    ))

    // Path to your JSON file
    val path = "path_to_your_file/users.json"

    // Read the JSON file with the defined schema
    val usersDf: DataFrame = spark.read
      .schema(userSchema) // specify the schema
      .json(path) // specify the input json file, this method must be placed at the end, it returns a DataFrame.

    // DOES NOT COMPILE:
/*    val usersDf1: DataFrame = spark.read
      .json(path) // specify the input json file
      .schema(userSchema) // specify the schema*/

    // Show the DataFrame content
    usersDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
