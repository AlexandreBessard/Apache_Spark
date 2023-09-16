  package com.sundogsoftware.spark.certification.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

  object Test18 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()

      //We want to create a new DataFrame with only the column "name" from an existing DataFrame

      // Sample DataFrame
      val data = Seq(
        (1, "John", 30),
        (2, "Jane", 25),
        (3, "Bob", 35)
      )

      val columns = Seq("id", "name", "age")

      // Create a DataFrame
      val df = spark.createDataFrame(data).toDF(columns: _*)

      // Select a specific column "a" from the DataFrame
      val selectedColumn = df.select("name")

      // Show the result
      selectedColumn.show()

      // Stop the SparkSession
      spark.stop()

    }
  }
