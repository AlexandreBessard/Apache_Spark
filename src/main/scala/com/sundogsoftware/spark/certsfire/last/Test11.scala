package com.sundogsoftware.spark.certsfire.last

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col // Import Spark's built-in SQL functions

object Test11 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    import spark.implicits._

    // Sample data for transactionsDf
    val transactionsData = Seq(
      (1, "apple", 5.0, 101),
      (2, "banana", 3.5, 102),
      (3, "cherry", 7.0, 101),
      (4, "date", 4.0, 103),
      (5, "fig", 2.5, 103)
    )

    val transactionsDf =
      transactionsData.toDF("transactionId", "itemName", "amount", "storeId")

    // Count the number of unique storeIds
    val uniqueStoreCount = {
      transactionsDf.select("storeId").dropDuplicates().count()
      //OR, other syntax
    }
    transactionsDf.select(col("storeId")).dropDuplicates().count()

    println(s"There are $uniqueStoreCount unique stores in the transactions dataset.")

    spark.stop()
  }
}
