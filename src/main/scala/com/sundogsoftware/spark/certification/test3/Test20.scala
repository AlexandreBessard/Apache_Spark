  package com.sundogsoftware.spark.certification.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

  object Test20 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()

      // Sample DataFrame
      val data = Seq(
        (1, "Alice", 1),
        (2, "Bob", 0),
        (3, "Carol", 3),
        (4, "David", 1)
      )

      val columns = Seq("id", "name", "count")

      // Create a DataFrame
      val df = spark.createDataFrame(data).toDF(columns: _*)

      // Use the `where` or `filter` method to filter rows
      val filteredDF = df.where("count < 2")

      //This code block throws an exception:
      // val filteredDF = df.select("count < 2")

      // Show the filtered DataFrame
      filteredDF.show()

      // Stop the SparkSession
      spark.stop()

    }
  }
