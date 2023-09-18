package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.{col, lit}

object Test9 {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Calculate square value of a specific column

    // Sample data
    val data = Seq(
      (1, "A", 1),
      (2, "B", 2),
      (3, "C", 3),
      (4, "D", 4)
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

    // Create a new column "predErrorSquared" containing squared values of "predError"
    val squaredDf = transactionsDf
      .withColumn("predErrorSquared",
        org.apache.spark.sql.functions.pow(col("predError"), lit(2)))

    // Display the DataFrame with the new "predErrorSquared" column
    println("DataFrame with Squared Prediction Errors:")
    squaredDf.show()
    // Stop the SparkSession
    spark.stop()
  }

}
