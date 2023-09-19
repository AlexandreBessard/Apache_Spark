package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

object Test15 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Return DataFrame in ascending order
     */

    // Sample data for transactionsDf
    val data = Seq(
      (1, "A", "100", "0.05"),
      (2, "B", "200", null),
      (3, "A", null, "0.1"),
      (4, "C", "300", "0.2"),
      (5, "null", null, null)
    )

    // Define the schema
    val schema = Seq(
      "transactionId", "storeId", "amount", "predError"
    )

    // Create a DataFrame with null values
    // Descending order means the bigger element first to the smallest element
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Display the original DataFrame
    println("Original DataFrame:")
    transactionsDf.show()

    // Sort the DataFrame by "predError" in descending order
    val sortedDf = transactionsDf.sort(col("predError").desc_nulls_first)

    // Sort the DataFrame by "predError" in descending order
    val sortedDf1 = transactionsDf.sort(col("predError").desc)

    // Display the sorted DataFrame
    println("DataFrame sorted by predError (Descending):")
    sortedDf1.show()

    // Display the sorted DataFrame
    println("DataFrame sorted by predError (Descending with nulls first):")
    sortedDf.show()

    // Stop the SparkSession
    spark.stop()
  }

}
