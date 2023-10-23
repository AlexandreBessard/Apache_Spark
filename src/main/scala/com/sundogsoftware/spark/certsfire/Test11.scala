package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col}  // Required for StringType

object Test11 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initializing SparkSession
    val spark = SparkSession.builder()
      .appName("ExplodeAttributesExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Sample data for the transactionsDf DataFrame
    val transactionsData = Seq(
      (1, "apple", 4.5, 25),
      (2, "banana", 3.0, 24),
      (3, "cherry", 2.5, 25),
      (4, "apple", 5.5, 25),
      (5, "date", 5.0, 26)
    )

    val transactionsDf = transactionsData.toDF("id", "item", "value", "storeId")

    // Display the original DataFrame (optional)
    println("Original DataFrame:")
    transactionsDf.show()

    // Grouping by `storeId` and calculating the average value
    val avgValuesDf = transactionsDf
      .groupBy(col("storeId"))
      .agg(avg(col("value")).alias("avgValue"))

    // Both syntax work as expected

    val avgValuesDf1 = transactionsDf
      .groupBy("storeId")
      .agg(avg("value").alias("avgValue"))

    // Display the result DataFrame
    println("Average Values by StoreId:")
    avgValuesDf.show()

    // Closing the SparkSession
    spark.close()
  }
}