package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, isnull}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test30 {

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test29")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data for transactionsDf and itemsDf
    val transactionsData = Seq(
      (1, 100, 10.0),
      (2, 200, 20.0),
      (3, 300, 30.0),
      (4, 400, 40.0)
    )

    val itemsData = Seq(
      (100, "ProductA"),
      (200, "ProductB"),
      (300, "ProductC"),
      (500, "ProductD") // Note: This item is not present in transactionsData
    )

    // Define schemas for the DataFrames
    import org.apache.spark.sql.types._
    val transactionsSchema = StructType(
      List(
        StructField("transactionId", IntegerType, nullable = false),
        StructField("productId", IntegerType, nullable = false),
        StructField("value", DoubleType, nullable = false)
      )
    )

    val itemsSchema = StructType(
      List(
        StructField("itemId", IntegerType, nullable = false),
        StructField("itemName", StringType, nullable = false)
      )
    )

    // Create DataFrames from the sample data with the defined schemas
    val transactionsDf: DataFrame = spark.createDataFrame(transactionsData).toDF("transactionId", "productId", "value")
    val itemsDf: DataFrame = spark.createDataFrame(itemsData).toDF("itemId", "itemName")

    // Perform the inner join on 'productId' and 'itemId'
    val joinedDf = transactionsDf.join(itemsDf, transactionsDf("productId") === itemsDf("itemId"), "inner")

    joinedDf.show()

    // Filter out rows where 'value' is not null and count the rows
    val rowCount = joinedDf.filter(!isnull(col("value"))).count()

    // Print the result
    println(s"Count of rows after inner join and filtering: $rowCount")

    // Stop the SparkSession
    spark.stop()
  }
}
