package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Test11 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    //Throws an error when writing an existing csv file.

    // Sample data
    val data = Seq(
      (1, "A", 2.5),
      (2, "B", 6.8),
      (3, "C", 1.2),
      (4, "D", 7.9),
      (5, "E", 4.5),
      (6, "F", 3.3)
    )

    // Define schema
    val schema = Seq(
      "id", "item", "predError"
    )

    // Create a DataFrame
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Display the original DataFrame
    println("Original DataFrame:")
    transactionsDf.show()

    // Write the DataFrame to a CSV file with error mode set to "error"
    transactionsDf.write
      .format("csv")
      /*
      The correct mode for handling errors during the write operation should be "error"
      (not "error"), which means that if the file already exists, it will raise an error.
       */
      .mode("error") // This should be "error" mode, not "error"
      .save("/FileStore/transactions.csv")

    //ignore mode:
    /*
    f the target file already exists, Spark will avoid overwriting
    it and will continue writing the remaining data.
    Any conflicting data will be left unchanged.
     */

    println("DataFrame has been written to CSV with error mode 'error'")

    // Stop the SparkSession
    spark.stop()
  }

}
