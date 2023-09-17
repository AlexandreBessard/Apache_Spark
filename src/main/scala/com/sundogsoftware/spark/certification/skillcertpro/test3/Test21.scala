  package com.sundogsoftware.spark.certification.skillcertpro.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

  object Test21 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()

      // Remove name and city from the DataFrame

      // Sample DataFrame
      val data = Seq(
        ("Alice", "New York", 30),
        ("Bob", "San Francisco", 25),
        ("Carol", "Los Angeles", 35)
      )

      val columns = Seq("name", "city", "age")

      // Create a DataFrame
      val df = spark.createDataFrame(data).toDF(columns: _*)

      // Drop the "name" and "city" columns
      val updatedDF = df.drop("name", "city")

      // Show the DataFrame with columns dropped
      updatedDF.show()

      // Stop the SparkSession
      spark.stop()

    }
  }
