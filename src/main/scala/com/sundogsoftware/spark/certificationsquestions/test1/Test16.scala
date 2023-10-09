package com.sundogsoftware.spark.certificationsquestions.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Test16 {

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data
    val transactionsData = Seq(
      (1, "A", 100),
      (2, "B", 200),
      (3, "C", 150)
    )

    val itemsData = Seq(
      (101, "Product1"),
      (102, "Product2"),
      (103, "Product3")
    )

    // Define the schemas
    val transactionsSchema = Seq(
      "transactionId", "productId", "amount"
    )

    val itemsSchema = Seq(
      "itemId", "itemName"
    )

    // Create DataFrames
    val transactionsDf = spark.createDataFrame(transactionsData).toDF(transactionsSchema: _*)
    val itemsDf = spark.createDataFrame(itemsData).toDF(itemsSchema: _*)

    // Display the original DataFrames
    println("transactionsDf:")
    transactionsDf.show()

    println("itemsDf:")
    itemsDf.show()

    // Perform an outer join on "productId" and "itemId"
    /*
     An outer join returns all rows from both tables, including the rows that don't have matching values
     in the joined columns. When there's no match for a row in one table, the result will contain null
     values for columns from that table.
     */
    val joinedDf = transactionsDf.join(itemsDf,
      transactionsDf("productId") === itemsDf("itemId"), "outer")

    // Display the result of the outer join
    println("DataFrame after outer join:")
    joinedDf.show()

    // Stop the SparkSession
    spark.stop()
  }

}
