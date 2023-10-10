package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test17 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Drop duplicates
     */

    // Sample data
    val data = Seq(
      (1, "ProductA"),
      (2, "ProductB"),
      (3, "ProductA"),
      (4, "ProductC"),
      (5, "ProductB"),
      (5, "ProductZ"),
      (6, "ProductZ") // Not removed because no rows have the same productId & transactionId
    )

    // Define the schema for the DataFrame
    val schema = List("transactionId", "productId")

    // Create a DataFrame from the sample data
    val transactionsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Drop duplicate rows based on "productId"
    val uniqueProductIdsDf = transactionsDf
      .dropDuplicates("productId", "transactionId")

    /*
    You are asking Spark to remove duplicate rows based on the combination of the values
    in the specified columns - in this case, "productId" and "transactionId".

    In simpler terms, imagine you have a list of transactions, and each transaction has an
    ID and is related to a specific product, which also has an ID. If two transactions have
    the same product ID and transaction ID, then they would be considered duplicates and one
    of them would be removed.
     */

    // Show the DataFrame with unique productIds
    uniqueProductIdsDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
