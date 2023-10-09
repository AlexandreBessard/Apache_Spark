package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, col}

object Test17 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    /*
    Perform a join in which the small DataFrame transactionId is sent to all executors.
     */

    // Sample data for itemsDf
    val itemsData = Seq(
      (1, "Product1"),
      (2, "Product2"),
      (3, "Product3"),
      (4, "Product4")
    )

    // Sample data for transactionsDf
    val transactionsData = Seq(
      (101, "A", 100),
      (102, "B", 200),
      (103, "C", 150),
      (4, "D", 200)
    )

    // Define the schemas
    val itemsSchema = Seq(
      "itemId", "itemName"
    )

    val transactionsSchema = Seq(
      "storeId", "productId", "amount"
    )

    // Create DataFrames
    val itemsDf = spark.createDataFrame(itemsData).toDF(itemsSchema: _*)
    val transactionsDf = spark.createDataFrame(transactionsData).toDF(transactionsSchema: _*)

    // Display the original DataFrames
    println("itemsDf:")
    itemsDf.show()

    println("transactionsDf:")
    transactionsDf.show()

    // Perform a broadcast join on "itemId" and "storeId"
    // The default is an inner join operation
    /*
    inner join:
    combine two sets of data based on a common key or column,
    and it returns only the rows that have matching values in both datasets
     */
    val joinedDf = itemsDf.join(broadcast(transactionsDf),
      itemsDf("itemId") === transactionsDf("storeId"))
    // Different syntax but same result as above
    val joinedDf1 = itemsDf.join(broadcast(transactionsDf),
      col("itemId") === col("storeId"))

    // Display the result of the broadcast join
    println("DataFrame after broadcast join:")
    joinedDf.show()

    joinedDf1.show()

    // Stop the SparkSession
    spark.stop()
  }

}
