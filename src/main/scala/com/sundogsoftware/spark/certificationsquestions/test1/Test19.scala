package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test19 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data for transactionsDf
    val data = Seq(
      (1, "A", 100, "2023-09-01"),
      (2, "B", 200, "2023-09-02"),
      (3, "C", 150, "2023-09-03"),
      (4, "A", 120, "2023-09-01"),
      (5, "B", 180, "2023-09-02")
    )

    // Define the schema
    val schema = Seq(
      "transactionId", "productId", "amount", "date"
    )

    // Create a DataFrame
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Display the original DataFrame
    println("Original DataFrame:")
    transactionsDf.show()

    // Write the DataFrame to Parquet format, partitioned by "storeId"
    transactionsDf.write
      .format("parquet")
      /*
      We specify the partitionBy("productId") option to partition the data by the
      "productId" column. This means that Parquet files will be written to separate
      directories based on the unique values in the "productId" column.
       */
      .partitionBy("productId") // Specify the column to partition by
      .save("/FileStore/transactions_split")

    // Stop the SparkSession
    spark.stop()
  }

}
