package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col, explode}

object Test13 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Return the average of rows in colum value grouped by unique storeId
     */

    // Sample data for transactionsDf
    val data = Seq(
      (1, "A", 100),
      (2, "B", 200),
      (3, "A", 150),
      (4, "B", 250),
      (5, "A", 120),
      (6, "C", 180)
    )

    // Define the schema
    val schema = Seq(
      "transactionId", "storeId", "value"
    )

    // Create a DataFrame
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Display the original DataFrame
    println("Original DataFrame:")
    transactionsDf.show()

    // Group by "storeId" and calculate the average of "value" for each store
    val resultDf = transactionsDf.groupBy("storeId").avg("value")

    // Rename the columns for clarity
    val finalResultDf = resultDf
      .withColumnRenamed("storeId", "StoreID")
      .withColumnRenamed("avg(value)", "AverageValue")

    // Display the final DataFrame
    println("DataFrame after groupBy and average calculation:")
    finalResultDf.show()

    // Stop the SparkSession
    spark.stop()
  }

}
