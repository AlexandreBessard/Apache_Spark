package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test26 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data for transactionsDf
    val data = Seq(
      (1, "A", 100, 10000.0),
      (2, "B", 200, 0.1),
      (3, "A", 100, 0.05),
      (4, "C", 300, 0.2),
      (5, "B", 200, 0.1),
      (1, "A", 500, 0.05),
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

    // Drop duplicate rows based on the "productId" column
    val deduplicatedDf = transactionsDf.dropDuplicates(Array("productId"))

    // Display the DataFrame after dropping duplicates
    println("DataFrame after dropping duplicates based on 'productId':")
    deduplicatedDf.show()

    // Stop the SparkSession
    spark.stop()
  }

}
