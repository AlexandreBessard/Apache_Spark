package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Test1 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initializing SparkSession
    val spark = SparkSession.builder()
      .appName("ExplodeAttributesExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Sample data for transactionsDf
    val transactionsData = Seq(
      (1, "itemA", 3),
      (2, "itemB", 4),
      (3, "itemC", 1),
      (4, "itemD", 5),
      (5, "itemE", 0),
      (6, "itemF", 2)
    )

    val transactionsDf = transactionsData.toDF("transactionId", "itemName", "productId")

    // Filter rows where productId is either 3 or less than or equal to 1
    val filteredDf =
      transactionsDf.filter((col("productId") === 3) || (col("productId") <= 1))

    val filteredDf1 =
      transactionsDf.filter((col("productId") === 3).or (col("productId") <= 1))

    // Must be a column when using "and" or "or"
    val filteredDf2 =
      transactionsDf.filter((col("productId") === 3).and (col("productId") <= 1))

    // Display the filtered data
    filteredDf.show()
    filteredDf1.show()
    filteredDf2.show()

    // Closing the SparkSession
    spark.close()
  }
}
