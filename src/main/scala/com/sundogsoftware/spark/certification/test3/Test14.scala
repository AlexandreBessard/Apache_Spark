  package com.sundogsoftware.spark.certification.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp


  object Test14 {

    def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("DataFrameColumnCasting")
        .master("local[*]") // Change this to your Spark cluster configuration
        .getOrCreate()

      // Sample data with a timestamp column
      val data = Seq(
        (1, Timestamp.valueOf("2023-09-13 10:30:00")),
        (2, Timestamp.valueOf("2023-08-25 14:45:00")),
        (3, Timestamp.valueOf("2023-07-17 18:15:00"))
      )

      val columns = Seq("ID", "Timestamp")

      // Create a DataFrame
      import spark.implicits._
      val df = data.toDF(columns: _*)

      // Use to_date and col functions to convert the Timestamp column to a Date column
      val resultDF: DataFrame = df.select(to_date(col("Timestamp"), "yyyy-MM-dd").as("Date"))

      // Show the result DataFrame
      resultDF.show()

      // Stop the SparkSession
      spark.stop()

    }
  }