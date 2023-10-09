  package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


  object Test1 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()

      // Define the schema explicitly
      val schema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("salary", IntegerType),
        StructField("bonus", IntegerType),
        StructField("location", StringType)
      ))

      // Sample data (replace with your actual data)
      val data = Seq(
        Row(1, "John", null, 25, null, "New York"),
        Row(2, "Alice", 30, null, 1200, "Chicago"),
        Row(3, null, null, null, null, null),
        Row(4, "Bob", 28, 3500, 850, "Los Angeles"),
        Row(5, "Alex", null, null, null, null),
      )

      // Create a DataFrame with the explicit schema
      val transDF = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

      // Drop rows with at least 4 non-null values
      // Meaning drop rows which contains 4 or more null values
      val filteredDF: DataFrame = transDF.na.drop(4)

      // Show the resulting DataFrame
      filteredDF.show()

      // Stop the SparkSession
      spark.stop()

    }
  }
