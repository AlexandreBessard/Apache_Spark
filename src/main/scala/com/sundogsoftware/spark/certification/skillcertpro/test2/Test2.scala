package com.sundogsoftware.spark.certification.skillcertpro.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, collect_list, sort_array, struct}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test2 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val rawData = Seq(
      (1, 1000, "Apple", 0.76),
      (2, 1000, "Apple", 0.11),
      (1, 2000, "Orange", 0.98),
      (1, 3000, "Banana", 0.24),
      (2, 3000, "Banana", 0.99)
    )

    // Create a DataFrame from the raw data
    val dfA: DataFrame = spark.createDataFrame(rawData).toDF("UserKey", "ItemKey", "ItemName", "Score")

    // Group by "UserKey" and aggregate the data
    val result: DataFrame = dfA
      // Group data by the user key column
      .groupBy("UserKey")
      .agg(
        sort_array(
          collect_list(
            struct(col("Score"), col("ItemKey"), col("ItemName"))
          ),
          // Bigger element to the smaller element
          false
        ).as("Collection")
      )
      .toDF("UserKey", "Collection")

    // Show the result
    result.show(20, false)

    // Stop the SparkSession
    spark.stop()

  }
}
