package com.sundogsoftware.spark.certification.skillcertpro.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object Test17 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample DataFrames with the same schema
    val df1 = spark.createDataFrame(Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie")
    )).toDF("id", "name")

    val df2 = spark.createDataFrame(Seq(
      (5, "David"),
      (4, "Eve")
    )).toDF("id", "test")

    // Perform a union between the two DataFrames
    // Must have the same schema
    val combinedDF: DataFrame = df1.union(df2)

    // Show the combined DataFrame
    combinedDF.show()

    // Stop the SparkSession
    spark.stop()

  }
}
