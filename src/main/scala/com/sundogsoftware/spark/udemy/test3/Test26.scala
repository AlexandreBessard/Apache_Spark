package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test26 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("ItemNameSeparatorExample")
      .master("local[*]")
      .getOrCreate()

    /*
    The code block shown below should return an exact copy of DataFrame
    transactionsDf that does not include rows in which values in column storeId have the value 25.
     Choose the answer that correctly fills the blanks in the code block to accomplish this.
     */

    // Sample data for transactionsDf
    val data = Seq(
      (1, 101, 10.0, 25),
      (2, 102, 15.0, 30),
      (3, 103, 12.0, 25),
      (4, 104, 8.0, 40),
      (5, 105, 18.0, 25)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "productId", "value", "storeId")

    // Create a DataFrame from sample data
    val transactionsDf: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Filter rows where "storeId" is not equal to 25
    // filter is used to filter rows, you can also use it by replacing where() by filter()
    val filteredDf = transactionsDf.where(transactionsDf("storeId") =!= 25)

    // Show the resulting DataFrame
    filteredDf.show()
    // Stop the SparkSession
    spark.stop()
  }
}
