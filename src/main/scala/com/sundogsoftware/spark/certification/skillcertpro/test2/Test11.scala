package com.sundogsoftware.spark.certification.skillcertpro.test2

import breeze.linalg.sum
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._


object Test11 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val a = Array("1002", "3001", "4002", "2003", "2002", "3004", "1003", "4006")

    // Create a DataFrame from the array and cast "value" to integers
    val df: DataFrame = spark.createDataFrame(a.map(value => (value.toInt, value.toInt % 1000))).toDF("value", "x")

    df.show()

    // Group by column "x" and calculate count and sum, then order by count and total
    val resultDf: DataFrame = df
      .groupBy(col("x")) // Group the dataframe by column x
      .agg(expr("count(x) as count"), expr("sum(value) as total"))
      //order by column count and column total
      .orderBy(col("count").desc, col("total"))
      .limit(2)
      .drop("x")

    //Without the drop method:
    /*
    +---+-----+-----+
    |  x|count|total|
    +---+-----+-----+
    |  2|    3| 7006|
    +---+-----+-----+
     */
    // Show the result
    resultDf.show()

    // Stop the SparkSession
    spark.stop()

  }
}
