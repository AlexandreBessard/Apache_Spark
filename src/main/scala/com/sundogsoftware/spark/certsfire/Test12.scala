package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col}  // Required for StringType

object Test12 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initializing SparkSession
    val spark = SparkSession.builder()
      .appName("ExplodeAttributesExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Sample data for the transactionsDf DataFrame
    val transactionsData = Seq(
      (1, "apple", 4.5, 25),
      (2, "banana", 3.0, 24),
      (3, "cherry", 2.5, 25)
    )

    val transactionsDf =
      transactionsData.toDF("id", "item", "value", "storeId")

    // Sample data for the transactionsNewDf DataFrame
    val transactionsNewData = Seq(
      (4, "date", 5.0, 26),
      (3, "cherry", 2.5, 25), // Duplicate entry, present in both DataFrames
      (5, "fig", 4.0, 27)
    )

    val transactionsNewDf =
      transactionsNewData.toDF("test1", "test2", "test3", "test4")

    // Union of both DataFrames and removal of duplicates
    // Even if the columns name are different, rows will be merged cause they match the type
    val mergedDf = transactionsDf.union(transactionsNewDf).distinct()

    // Display the merged DataFrame
    println("Merged DataFrame without Duplicates:")
    mergedDf.show()

    // Closing the SparkSession
    spark.close()
  }
}