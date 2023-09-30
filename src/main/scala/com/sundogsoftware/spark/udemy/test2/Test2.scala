package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}

object Test2 {

  Logger.getLogger("org").setLevel(Level.ERROR)


  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data (replace this with your actual DataFrame)
    val data = Seq(
      (1, "ProductA", 100),
      (2, "ProductB", 200),
      (3, "ProductA", 100),
      (4, "ProductC", 300),
      (5, "ProductB", 200)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "itemName", "storeId")

    // Create a DataFrame from the sample data
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Select the "storeId" column and get distinct values
    val distinctStoreIdsDf = transactionsDf.select("storeId").distinct()

    // Show the DataFrame with distinct store IDs
    distinctStoreIdsDf.show()


    // Stop the SparkSession
    spark.stop()
  }

}
