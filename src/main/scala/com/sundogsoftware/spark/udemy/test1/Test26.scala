package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit


object Test26 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data (replace this with your actual DataFrame)
    val data = Seq(
      (1, 100, "ProductA", 10.5),
      (2, 200, "ProductB", 15.2),
      (3, 300, "ProductA", 12.8),
      (4, 400, "ProductC", 8.3),
      (5, 500, "ProductB", 9.7)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "productId", "itemName", "value")

    // Create a DataFrame from the sample data
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Add a new column 'associateId' with a constant value of 5
    val dfWithAssociateId = transactionsDf
      .withColumn("associateId", lit(5))

    // Drop the 'productId' and 'value' columns
    val dfWithoutColumns = dfWithAssociateId
      .drop("productId", "value")

    // Show the DataFrame after adding and dropping columns
    dfWithoutColumns.show()

    // Stop the SparkSession
    spark.stop()
  }
}
