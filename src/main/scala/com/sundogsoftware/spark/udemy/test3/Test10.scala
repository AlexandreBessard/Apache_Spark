package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{asc, desc, desc_nulls_last}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test10 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test6")
      .master("local[*]")
      .getOrCreate()

    /*
    Illustrate the method "orderBy"
     */

    // TODO: need to be reviewed

    // Sample data (Replace with your actual data or DataFrame)
    val data = Seq(
      ("Alice", 25),
      ("Bob", 30),
      ("Charlie", 22),
      ("David", 35),
      ("Eve", 28)
    )

    // Define the schema for the DataFrame
    val schema = List("Name", "Age")

    // Create a DataFrame from the sample data
    val df: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Order the DataFrame by the 'Age' column in ascending order
    val ascSortedDf = df.orderBy(asc("Age"))

    // Show the DataFrame sorted in ascending order
    println("Ascending Order:")
    ascSortedDf.show()

    // Order the DataFrame by the 'Age' column in descending order
    val descSortedDf = df.orderBy(desc("Age"))

    // Show the DataFrame sorted in descending order
    println("Descending Order:")
    descSortedDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
