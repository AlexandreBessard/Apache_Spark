package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Test2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data with missing values
    val data = Seq(
      (1, "A", "100", "2023-09-01"),
      (2, null, null, "2023-09-02"),
      (3, "C", null, null),
      (4, "D", "400", "2023-09-04")
    )

    // Define schema
    val schema = StructType(Seq(
      StructField("transactionId", IntegerType, false), // not nullable
      StructField("itemId", StringType, true),
      StructField("amount", StringType, true),
      StructField("date", StringType, true)
    ))

    // Create a DataFrame with the explicit schema
    val rowRDD = spark.sparkContext.parallelize(data).map {
      case (transactionId: Int, itemId: String, amount: String, date: String) =>
        Row(transactionId, itemId, amount, date)
    }

    val transactionsDf = spark.createDataFrame(rowRDD, schema)

    // Display the original DataFrame
    println("Original DataFrame:")
    transactionsDf.show()

    // Drop rows with fewer than 4 non-null values
    val resultDf = dropRowsWithThreshold(transactionsDf, 2)

    // Display the result DataFrame
    println("DataFrame after dropping rows with fewer than 4 non-null values:")
    resultDf.show()

    // Stop the SparkSession
    spark.stop()
  }

  private def dropRowsWithThreshold(df: DataFrame, threshold: Int): DataFrame = {
    df.na.drop(threshold)
  }
}
