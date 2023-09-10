package com.sundogsoftware.spark.certification.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, date_format}
import org.apache.spark.sql.types.IntegerType

object Test10 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample DataFrame
    val data = Seq(
      ("Alice", "25"),
      ("Bob", "30"),
      ("Charlie", "35")
    )
    val df = spark.createDataFrame(data).toDF("name", "bonus")

    // Example 1: Casting the column to IntegerType using col() and cast()
    val dfWithCasting1: DataFrame = df.withColumn("bonus", col("bonus").cast(IntegerType))

    // Show the DataFrame after casting
    dfWithCasting1.show()

    // Example 2: Casting the column to IntegerType using a string type name
    val dfWithCasting2: DataFrame = df.withColumn("bonus", col("bonus").cast("integer"))

    // Show the DataFrame after casting
    dfWithCasting2.show()

    // Stop the SparkSession
    spark.stop()

  }
}
