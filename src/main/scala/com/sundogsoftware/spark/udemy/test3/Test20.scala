package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test20 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("ItemNameSeparatorExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data
    val data = Seq(
      (1, "ProductA", 101),
      (2, "ProductB", 102),
      (3, "ProductA", 103),
      (4, "ProductC", 104),
      (5, "ProductB", 105),
      (6, "Test", 101),
      (7, "ProductA", 101)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "productName", "productId")

    // Create a DataFrame from the sample data
    val transactionsDf: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    transactionsDf.show()
    println("-----> example using groupBy")
    // Same result as shown below
    // count returns a DataFrame
    transactionsDf.groupBy("productId").count().show()

    /*
    +---------+-----+
    |productId|count|
    +---------+-----+
    |      101|    2|
    |      103|    1|
    |      102|    1|
    |      105|    1|
    |      104|    1|
    +---------+-----+
     */

    // Group by "productId" and count the number of occurrences
    val resultDf = transactionsDf
      .groupBy("productId")
      .agg(count("*").alias("count"))

    val resultDf1 = transactionsDf
      .groupBy("productName")
      .agg(count("productId").alias("count"))


    // Show the resulting DataFrame
    resultDf.show()
    resultDf1.show()

    // Stop the SparkSession
    spark.stop()
  }
}
