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

    // TODO: Need to be reviewed

    // Sample data
    val rawData = Seq(
      (1, 1000, "Apple", 0.76),
      (2, 1000, "Apple", 0.11),
      (1, 2000, "Orange", 0.76),
      (1, 3000, "Banana", 0.24),
      (2, 3000, "Banana", 0.99)
    )

    // Create a DataFrame from the raw data
    val dfA: DataFrame = spark.createDataFrame(rawData)
      .toDF("UserKey", "ItemKey", "ItemName", "Score")

    // Group by "UserKey" and aggregate the data
    val result: DataFrame = dfA
      // Group data by the user key column
      .groupBy("UserKey")
      .agg(
        sort_array(
          collect_list(
            // Order based on Score first, if same value sort based on ItemKey and so on ...
            struct(col("Score"), col("ItemKey"), col("ItemName"))
          ),
          // Bigger element to the smaller element
          asc = false // desc order
        ).as("Collection")
      )
      .toDF("UserKey", "Collection")

    // Show the result
    // truncate – Whether truncate long strings.
    // If true, strings more than 20 characters will be truncated and
    // all cells will be aligned right
    result.show(20, truncate = false)
    // OR
    result.show(20, false)

    // Stop the SparkSession
    spark.stop()

  }
}
