package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Test22 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data for transactions: (transactionId, storeId, productId, amount)
    val transactions = List(
      (1, "storeA", 2, 100.0),
      (2, "storeB", 2, 150.0),
      (3, "storeA", 3, 110.0),
      (4, "storeC", 4, 120.0),
      (5, "storeB", 4, 130.0)
    )

    import spark.implicits._
    val transactionsDf = transactions.toDF("transactionId", "storeId", "productId", "amount")

    // Drop duplicates based on "productId" column
    val deduplicatedDf = transactionsDf.dropDuplicates("storeId" :: Nil)

    // Display the DataFrame
    deduplicatedDf.show()

    // Stop Spark session
    spark.stop()
  }
}
