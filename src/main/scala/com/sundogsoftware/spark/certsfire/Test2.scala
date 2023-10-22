package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc}

object Test2 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initializing SparkSession
    val spark = SparkSession.builder()
      .appName("ExplodeAttributesExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    /*
    Sort in ascending order the first column and then descending order by the other column.
    Executed by priority.
     */

    // Sample data for transactionsDf
    val transactionsData = Seq(
      (1, "itemA", 3, 1001),
      (2, "itemB", 5, 1002),
      (3, "itemC", 1, 1001),
      (4, "itemD", 4, 1003),
      (5, "itemE", 0, 1002),
      (6, "itemF", 2, 1001)
    )

    val transactionsDf = transactionsData.toDF("transactionId", "itemName", "productId", "storeId")

    // Sorting the DataFrame by storeId in ascending order, and by productId in descending order
    val sortedDf = transactionsDf.sort($"storeId", desc("productId"))
    // First column will be sorted in ascending order by default
    val sortedDf1 = transactionsDf.sort(col("storeId"), desc("productId"))

    // Display the sorted data
    sortedDf.show()
    sortedDf1.show()

    // Closing the SparkSession
    spark.close()
  }
}
