package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test9 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample DataFrame (replace this with your actual DataFrame)
    val data = Seq(
      ("ProductA", 100.0),
      ("ProductB", 150.0),
      ("ProductC", 75.0)
    )
    val schema = List("Product", "SalesAmount")
    val salesDF: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Calculate total and average sales amount using DataFrame.agg()
    val aggregationResult = salesDF.agg(
      org.apache.spark.sql.functions.sum("SalesAmount").alias("TotalSales"),
      avg("SalesAmount").alias("AverageSales")
    )

    // Show the aggregation result
    aggregationResult.show()

    // Sample DataFrame (replace this with your actual DataFrame)
    // Your existing code
    val data1 = Seq(
      ("CategoryA", "ProductA", 100.0),
      ("CategoryA", "ProductB", 150.0),
      ("CategoryB", "ProductC", 75.0),
      ("CategoryA", "ProductD", 120.0)
    )
    val schema1 = List("Category", "Product", "SalesAmount")
    val salesDF1: DataFrame = spark.createDataFrame(data1).toDF(schema1: _*)

    // Calculate total sales amount for each product category using DataFrame.groupBy().agg()
    val categoryTotalSales = salesDF1.groupBy("Category")
      .agg(org.apache.spark.sql.functions.sum("SalesAmount").alias("TotalSales"))

    // Show the aggregation result
    categoryTotalSales.show()

    // Stop the SparkSession
    spark.stop()
  }
}
