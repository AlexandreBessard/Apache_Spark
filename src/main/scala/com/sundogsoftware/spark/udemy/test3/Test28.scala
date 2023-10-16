package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{cos, degrees, round}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test28 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    /*
    comment option allows you to specify a character or string that indicates comments
    within the CSV file. Comments are lines in the file that should be ignored during
    the reading process. Any line in the CSV file that starts with the specified comment
    character or string will be treated as a comment and won't be included in the resulting DataFrame.
     */

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("ItemNameSeparatorExample")
      .master("local[*]")
      .getOrCreate()

    // Define the file path
    val filePath = "/home/alex/Dev/Apache_Spark/SparkScalaCourse/src/main/scala/com/sundogsoftware/spark/udemy/test3/test28.csv"

    // Read the CSV file into a DataFrame
    val df = spark
      .read
      .option("comment", "#") // Specify the comment character
      .csv(filePath)

    // Count the number of columns
    val columnCount = df.columns.length

    // Print the column count
    println(s"Number of columns: $columnCount")

    // Stop the SparkSession
    spark.stop()
  }
}
