package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Test4 {

  Logger.getLogger("org").setLevel(Level.ERROR)


  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data (replace this with your actual DataFrame)
    val data = Seq(
      (1, "ProductA", 3),
      (2, "ProductB", 2),
      (3, "ProductA", 1),
      (4, "ProductC", 4),
      (5, "ProductB", 5),
      (1, "ProductA", -10),
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "itemName", "productId")

    // Create a DataFrame from the sample data
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Filter the DataFrame based on the condition
    // Keep rows in they meet the conditions (true)
    val filteredDf = transactionsDf
      .filter((col("productId") === 3) || (col("productId") < 1))

    val filteredDf1 = transactionsDf
      .filter((transactionsDf("productId") === 3).or(col("productId") < 1))

    // Show the filtered DataFrame
    filteredDf.show()
    filteredDf1.show()

    // Stop the SparkSession
    spark.stop()
  }

}
