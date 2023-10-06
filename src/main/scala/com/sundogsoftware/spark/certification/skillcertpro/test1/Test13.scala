package com.sundogsoftware.spark.certification.skillcertpro.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Test13 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data for cities and their populations
    val data = Seq(
      // When using StructType, we have to use a Row object for each row
      Row("New York", 8622698),
      Row("Los Angeles", 3999759),
      Row("Chicago", 2716450),
      Row("Houston", 2320268),
      Row("Phoenix", 1680992),
      Row("Paris", 2140526),
      Row(null, 0) //
      // Adding a row with null city and population
    )

    // Define the schema for the DataFrame with two columns
    val schema = StructType(Seq(
      StructField("city", StringType, true), // "true" allows null values
      StructField("population", IntegerType, true)
    ))

    // Create a DataFrame using the schema and rows
    val cityPopulationDF =
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // Register the DataFrame as a temporary SQL table
    cityPopulationDF.createOrReplaceTempView("cityPopulation")

    // Perform SQL queries on the DataFrame
    // Select cities with a population greater than 2 million
    val resultDF = spark
      .sql("SELECT city FROM cityPopulation WHERE population > 2000000")

    // Show the result
    resultDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
