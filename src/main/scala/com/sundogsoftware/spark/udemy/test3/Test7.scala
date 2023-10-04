package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ArrayType, IntegerType}

object Test7 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test6")
      .master("local[*]")
      .getOrCreate()

    /*
    Which of the following code blocks performs an inner join of DataFrames transactionsDf and itemsDf
    on columns productId and itemId, respectively, excluding columns value and storeId from DataFrame
    transactionsDf and column attributes from DataFrame itemsDf?
     */

    // Sample data (Replace with your actual data or DataFrames)
    val transactionsData = Seq(
      (1, 101, 5.0),
      (2, 102, 4.5),
      (3, 103, 3.0)
    )

    val itemsData = Seq(
      (101, "ProductA", "Category1"),
      (102, "ProductB", "Category2"),
      (103, "ProductC", "Category1")
    )

    // Define the schema for the DataFrames
    val transactionsSchema = List("transactionId", "productId", "value")
    val itemsSchema = List("itemId", "itemName", "category")

    // Create DataFrames from the sample data
    val transactionsDf: DataFrame = spark.createDataFrame(transactionsData).toDF(transactionsSchema: _*)
    val itemsDf: DataFrame = spark.createDataFrame(itemsData).toDF(itemsSchema: _*)

    // Create temporary views for the DataFrames
    transactionsDf.createOrReplaceTempView("transactionsDf")
    itemsDf.createOrReplaceTempView("itemsDf")

    // Define the SQL statement for the inner join
    val statement =
      """
        |SELECT * FROM transactionsDf
        |INNER JOIN itemsDf
        |ON transactionsDf.productId == itemsDf.itemId
  """.stripMargin

    // Execute the SQL statement and drop unnecessary columns
    val joinedDf = spark.sql(statement).drop("value", "storeId", "attributes")

    // Show the resulting DataFrame
    joinedDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
