package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

object Test10 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val data = Seq(
      (1, "A", 2.5),
      (2, "B", 6.8),
      (3, "C", 1.2),
      (4, "D", 7.9),
      (5, "E", 4.5),
      (6, "F", 3.3)
    )

    // Define schema
    val schema = Seq(
      "id", "item", "predError"
    )

    // Create a DataFrame
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Display the original DataFrame
    println("Original DataFrame:")
    transactionsDf.show()

    // Filter the DataFrame to select rows with "predError" >= 5
    val filteredDf = transactionsDf.where("predError >= 5")
    // Equivalent but filter takes a col as argument, not where()
    val filteredDf1 = transactionsDf.filter(col("predError") >= 5)

    val filteredDf2 = transactionsDf.filter(transactionsDf("predError") >= 5)

    // Display the filtered DataFrame
    println("DataFrame with PredError >= 5:")
    filteredDf.show()
    filteredDf1.show()
    filteredDf2.show()

    // Stop the SparkSession
    spark.stop()
  }

}
