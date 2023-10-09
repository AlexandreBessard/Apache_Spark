package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}

object Test5 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data with nested arrays
    val data = Seq(
      (1, "A", Array("X", "Y", "Z")),
      (2, "B", Array("P", "Q")),
      (3, "C", Array("R")),
      (4, "D", Array("S", "T", "U"))
    )

    // Define schema
    val schema = Seq(
      "id", "value", "arrayColumn"
    )

    // Create a DataFrame
    val df = spark.createDataFrame(data).toDF(schema: _*)

    // Display the original DataFrame
    println("Original DataFrame:")
    df.show()

    // Use select() with explode() to unnest the arrayColumn and select specific columns
    val selectedExplodedDf = df
      .select(col("id"), col("value"), explode(col("arrayColumn")).as("explodedValue"))

    // Column name will be named "col"
    val selectedExplodedDf1 = df
      .select(col("id"), col("value"), explode(col("arrayColumn").alias("explodedValue")))

    // Display the selected DataFrame after using select() and explode()
    println("Selected DataFrame after using select() with explode():")
    selectedExplodedDf.show()
    selectedExplodedDf1.show()

    // Stop the SparkSession
    spark.stop()
  }
}
