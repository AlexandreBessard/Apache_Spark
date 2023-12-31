package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test7 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Which of the following code blocks saves DataFrame transactionsDf in location
    /FileStore/transactions.csv as a CSV file and throws an error if a file already exists in the location
     */

    // Sample DataFrame (replace this with your actual DataFrame)
    val data = Seq(
      (1, "Alice", 30),
      (2, "Bob", 25),
      (3, "Charlie", 35)
    )
    val schema = List("id", "name", "age")
    val transactionsDF: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

    // Write the DataFrame to a CSV file with mode("error")
    transactionsDF.write
      .format("csv")
      .mode("error") // This sets the mode to "error"
      // load when reading and save when writing
      .save("/FileStore/transactions.csv") // Replace with your desired file path

    /*
    error mode:
    If the data/file/directory exists: Spark will not overwrite it and will
    throw an error instead, stopping the job. It essentially says "Hey, there’s
    already data here, and I won’t overwrite it because you asked me to error out in this situation!"
     */

    // Stop the SparkSession
    spark.stop()
  }
}
