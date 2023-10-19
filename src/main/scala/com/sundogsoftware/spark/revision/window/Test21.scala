package com.sundogsoftware.spark.revision.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Test21 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExplodeExample")
      .master("local[*]")
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data for transactions: (transactionId, storeId, productId, amount)
    val transactions = List(
      (1, 15, 2, 100.0),
      (2, 22, 2, 150.0),
      (3, 25, 3, 110.0),
      (4, 32, 2, 120.0),
      (5, 28, 2, 130.0)
    )

    import spark.implicits._
    val transactionsDf = transactions.toDF("transactionId", "storeId", "productId", "amount")

    val resultDf = transactionsDf
      .select(col("storeId"), col("productId"))
      .filter(col("storeId").between(20, 30) && col("productId") === 2)

    // Display the filtered DataFrame
    resultDf.show()

    // Stop Spark session
    spark.stop()
  }
}
