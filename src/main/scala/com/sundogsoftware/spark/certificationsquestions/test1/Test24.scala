package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test24 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Display 10 rows with the smallest values of column value in DataFrame
     */

    // Sample data for transactionsDf
    val data = Seq(
      (1, "A", "100", "0.05"),
      (2, "B", "200", null),
      (3, "A", null, "0.1"),
      (4, "C", "300", "0.2"),
      (5, "null", "400", null)
    )

    // Define the schema
    val schema = Seq(
      "transactionId", "productId", "amount", "value"
    )

    // Create a DataFrame
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Display the original DataFrame
    println("Original DataFrame:")
    transactionsDf.show()

    // Sort the DataFrame by the "value" column in ascending order and show the top 10 rows
    transactionsDf.sort(col("value")).show(10)

    // Stop the SparkSession
    spark.stop()
  }

}
