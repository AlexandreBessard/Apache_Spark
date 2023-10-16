package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, from_unixtime, unix_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test16 {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test6")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data (Replace with your actual data or DataFrame)
    val data = Seq(
      (1, "2023-09-15 08:30:45"),
      (2, "2023-09-16 12:15:30"),
      (3, "2023-09-17 16:45:20")
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "transactionDate")

    // Create a DataFrame from the sample data
    val transactionsDf: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Apply the from_unixtime function to convert Unix timestamp to formatted date
    /*
    we use the unix_timestamp function to explicitly parse the "transactionDate"
    column as a timestamp with the specified format before applying from_unixtime
    for formatting.
    Make sure that your "transactionDate" column matches the expected format.
     */
    val formattedDf = transactionsDf
      .withColumn("transactionDateForm",
      from_unixtime(
        //Convert the string as a timestamp. The format must match the DataFrame.
        unix_timestamp(col("transactionDate"), "yyyy-MM-dd HH:mm:ss"), "MMM d (EEEE)"))

    // Show the resulting DataFrame
    formattedDf.show()

    /*
    +-------------+-------------------+-------------------+
    |transactionId|    transactionDate|transactionDateForm|
    +-------------+-------------------+-------------------+
    |            1|2023-09-15 08:30:45|    Sep 15 (Friday)|
    |            2|2023-09-16 12:15:30|  Sep 16 (Saturday)|
    |            3|2023-09-17 16:45:20|    Sep 17 (Sunday)|
    +-------------+-------------------+-------------------+
     */

    // Stop the SparkSession
    spark.stop()
  }
}
