package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc}

object Test5 {

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
      (5, "ProductB", 5)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "itemName", "productId")

    // Create a DataFrame from the sample data
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Sort the DataFrame by "itemName" in ascending order and "productId" in descending order
    val sortedDf = transactionsDf
      .sort("itemName") // asc order first
      .sort(desc("productId")) // take precedence over the first sort()

    // Show the sorted DataFrame
    sortedDf.show()

    // Stop the SparkSession
    spark.stop()
  }

}
