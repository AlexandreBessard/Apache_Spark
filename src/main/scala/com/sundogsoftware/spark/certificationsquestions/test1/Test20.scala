package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Test20 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Delete multiple columns from a DataFrame.
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
      "transactionId", "productId", "amount", "predError"
    )

    // Create a DataFrame
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Display the original DataFrame
    println("Original DataFrame:")
    transactionsDf.show()

    // Drop specified columns from the DataFrame
    val resultDf = transactionsDf.drop("predError", "productId", "amount")

    // DOES NOT COMPILE
/*    val resultDf1 = transactionsDf
      .drop(col("predError"), col("productId"), col("amount"))*/

    // Display the DataFrame after dropping columns
    println("DataFrame after dropping columns:")
    resultDf.show()

    // Stop the SparkSession
    spark.stop()
  }

}
