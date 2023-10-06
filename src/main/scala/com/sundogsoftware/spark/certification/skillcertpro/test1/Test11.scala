package com.sundogsoftware.spark.certification.skillcertpro.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StructField,StringType, StructType}
import org.apache.spark.storage.StorageLevel

object Test11 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Create a list of cities
    val cityList = Seq("New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Paris", null)

    // Define the schema for the DataFrame
    // true -> means it can be nullable.
    // If set to false, throws an exception because the value can not be null:
    // RuntimeException: Error while encoding: RuntimeException: The 0th field 'city' of input row cannot be null.
    val schema = StructType(Seq(StructField("city", StringType, true)))

    // Create Rows from the city list
    val rows = cityList.map(city => Row(city))

    // Create a DataFrame using the schema and rows
    val cityDF = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

    cityDF.show()

    // Register the DataFrame as a temporary SQL table
    cityDF.createOrReplaceTempView("cityList")

    // Perform the SQL query to select cities with a length of 5 characters
    val resultDF = spark.sql("SELECT city FROM cityList WHERE LENGTH(city) == 5")

    // Show the result
    resultDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
