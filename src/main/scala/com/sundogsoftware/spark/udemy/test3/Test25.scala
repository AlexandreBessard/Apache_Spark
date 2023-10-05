package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test25 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("ItemNameSeparatorExample")
      .master("local[*]")
      .getOrCreate()

    /*
    The code block shown below should return a two-column DataFrame with columns transactionId and
    supplier, with combined information from DataFrames itemsDf and transactionsDf.
    The code block should merge rows in which column productId of DataFrame transactionsDf
    matches the value of column itemId in DataFrame itemsDf, but only where column storeId of
    DataFrame transactionsDf does not match column itemId of DataFrame itemsDf.
     */

    // Sample data for itemsDf and transactionsDf
    val itemsData = Seq(
      (1, "ProductA", "Sports Company Inc."),
      (2, "ProductB", "YetiX"),
      (3, "ProductC", "Sports Company Inc.")
    )

    val transactionsData = Seq(
      (101, 1, 10.0),
      (102, 2, 15.0),
      (103, 3, 12.0),
      (104, 4, 8.0),
      (105, 5, 18.0)
    )

    // Define schemas for the DataFrames
    val itemsSchema = List("itemId", "itemName", "supplier")
    val transactionsSchema = List("transactionId", "productId", "value")

    // Create DataFrames from sample data
    val itemsDf: DataFrame = spark.createDataFrame(itemsData).toDF(itemsSchema: _*)
    val transactionsDf: DataFrame = spark.createDataFrame(transactionsData).toDF(transactionsSchema: _*)

    // Perform the merge based on the specified conditions
    val mergedDf = transactionsDf.join(
      itemsDf,
      transactionsDf("productId") === itemsDf("itemId"),
      "inner"
    ).select("transactionId", "supplier")

    // Show the resulting DataFrame
    mergedDf.show()
    // Stop the SparkSession
    spark.stop()
  }
}
