package com.sundogsoftware.spark.certification.skillcertpro.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Test15 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val data = Seq(
      ("Alice", 1),
      ("Bob", 3),
      ("Charlie", 0),
      ("David", 2)
    )

    // Create a DataFrame from the sample data with column names
    val df: DataFrame = spark.createDataFrame(data).toDF("name", "count")

    // Show the original DataFrame
    println("Original DataFrame:")
    df.show()

    // Filter the DataFrame to keep rows where "count" is less than 2
    val filteredDf: DataFrame = df.where("count < 2")

    // Show the filtered DataFrame
    println("Filtered DataFrame:")
    filteredDf.show()

    // Stop the SparkSession
    spark.stop()

  }
}
