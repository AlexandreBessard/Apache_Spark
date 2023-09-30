package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, substring_index}

object Test1 {

  Logger.getLogger("org").setLevel(Level.ERROR)


  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data (replace this with your actual DataFrame)
    val data = Seq(
      (1, Array("blue", "winter", "cozy")),
      (2, Array("red", "summer", "fresh")),
      (3, Array("green", "summer", "travel"))
    )

    // Define the schema for the DataFrame
    val schema = List("itemId", "attributes")

    // Create a DataFrame from the sample data
    val itemsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Use explode to split the "attributes" array column
    val explodedDf = itemsDf
      .select(explode(col("attributes")).alias("attributes_exploded"))
      // Filter for values containing the letter "i"
      .filter(col("attributes_exploded").contains("i"))

    // Show the filtered DataFrame
    explodedDf.show()

    // Stop the SparkSession
    spark.stop()
  }

}
