package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test21 {
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
      (1, 101, 2, 5.0, 10),
      (2, 102, 2, 4.5, 20),
      (3, 101, 3, 4.2, 15),
      (4, 103, 2, 4.8, 25),
      (5, 102, 3, 5.0, 18),
      (6, 101, 2, 4.6, 12)
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "storeId", "productId", "predError", "value")

    // Create a DataFrame from the sample data
    val transactionsDf: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    transactionsDf.show()

    // Filter out rows where storeId is not null
    val filteredDf = transactionsDf.filter(transactionsDf("storeId").isNotNull)

    filteredDf.show()

    // Group by "storeId", pivot on "productId", calculate mean of "predError", and order by "storeId"
    val resultDf = filteredDf
      .groupBy("storeId")
      /*
      In this context, it means that you want to create new columns in the resulting DataFrame
      for the distinct values "2" and "3" found in the "productId" column.
      These columns will represent aggregated values (in this case, the mean of "predError")
      for each combination of "storeId" and "productId" where "productId" is either 2 or 3.

      So, the resulting DataFrame will have columns like "2_mean(predError)" and "3_mean(predError)",
      where the values represent the mean of "predError" for each "storeId" and each "productId"
      with values 2 or 3. This is a way to pivot the data and summarize it based on specific values
      in the "productId" column.
       */
      // Before pivot, you have to use groupBy
      .pivot("productId", Seq(2, 3))
      .agg(mean("predError"))
      .orderBy("storeId") // orderBy() returns a DataSet, ascending order

    resultDf.show()

    /*
    // Original DataFrame
    +-----+------+-----+
    |Year |Month |Sales|
    +-----+------+-----+
    |2020 |Jan   |100  |
    |2020 |Feb   |120  |
    |2021 |Jan   |130  |
    |2021 |Feb   |110  |
    +-----+------+-----+

    // Using pivot
    val pivotedDF = originalDF.groupBy("Year").pivot("Month").sum("Sales")

    // Resulting DataFrame
    +-----+-----+-----+
    |Year |Jan  |Feb  |
    +-----+-----+-----+
    |2020 |100  |120  |
    |2021 |130  |110  |
    +-----+-----+-----+
     */

    // Stop the SparkSession
    spark.stop()
  }
}
