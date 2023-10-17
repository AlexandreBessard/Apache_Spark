package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, split}

object Test17 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Sample transaction data: (transactionId, userId, amount)
    val transactions = List(
      (1, 101, 250.0),
      (2, 102, 300.0),
      (3, 103, 100.0),
      (4, 104, 50.0)
    )

    // Convert the list to a DataFrame
    import spark.implicits._
    val transactionsDf = transactions
      .toDF("transactionId", "userId", "amount")

    // Display the DataFrame (optional)
    transactionsDf.show()

    // Write the DataFrame to CSV format
    transactionsDf
      .write
      .format("csv")
      .mode("error")
      .save("/FileStore/transactions.csv") // TODO: warning, it is save, NOT path


    // Stop Spark session
    spark.stop()
  }
}
