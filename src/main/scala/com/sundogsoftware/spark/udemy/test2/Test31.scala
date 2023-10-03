package com.sundogsoftware.spark.udemy.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, isnull}
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object Test31 {

  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Test29")
      .master("local[*]")
      .getOrCreate()

    // Sample data for transactionsDf
    val data = Seq(
      (1, "ProductA", 3.0),
      (2, "ProductB", 2.5),
      (3, "ProductA", 1.8),
      (4, "ProductC", 4.2),
      (5, "ProductB", 3.7)
    )

    // Define the schema for the DataFrame
    import org.apache.spark.sql.types._
    val schema = StructType(
      List(
        StructField("transactionId", IntegerType, nullable = false),
        StructField("itemName", StringType, nullable = false),
        StructField("predError", DoubleType, nullable = false)
      )
    )

    // Create a DataFrame from the sample data with the defined schema
    val transactionsDf: DataFrame = spark.createDataFrame(data).toDF("transactionId", "itemName", "predError")

    // Filter the DataFrame to count rows where 'predError' is 3 or 6
    /*
    The isin function generates a Boolean condition that evaluates to true
    for rows where the column's value matches any of the provided values and false otherwise.
     */
    val filteredRowCount = transactionsDf.filter(col("predError").isin(3, 6)).count()

    // Print the result
    println(s"Count of rows where 'predError' is 3 or 6: $filteredRowCount")


    // Stop the SparkSession
    spark.stop()
  }
}
