  package com.sundogsoftware.spark.certification.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


  object Test24 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()

      //Remove any rows that contains a null value

      // Sample DataFrame with null values
      val data = Seq(
        (1, "Alice", 30),
        (2, "Bob", null.asInstanceOf[Int]),
        (3, "Carol", 35),
        (4, null.asInstanceOf[String], 28),
        (5, "Eve", 32)
      )

      val columns = Seq("ID", "Name", "Age")

      // Create a DataFrame
      val df = spark.createDataFrame(data).toDF(columns: _*)

      // Use na.drop("any") to remove rows with any null or NaN values
      /*
      If how is "any", then drop rows containing any null or NaN values.
      If how is "all", then drop rows only if every column is null or NaN for that row.
       */
      val dfWithoutNulls: DataFrame = df.na.drop("any")

      // Show the DataFrame after dropping rows
      dfWithoutNulls.show()

      // Stop the SparkSession
      spark.stop()

    }
  }
