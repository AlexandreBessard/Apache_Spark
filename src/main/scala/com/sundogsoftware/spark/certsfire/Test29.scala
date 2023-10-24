package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count  // Only importing the necessary functions

object Test29 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("SplitExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // TODO: need to be reviewed

    // Sample data for transactionsDf
    val transactionsData = Seq(
      (1, "item1"),
      (2, "item2"),
      (1, "item3"),
      (2, "item4"),
      (3, "item5")
    )

    val transactionsDf = transactionsData.toDF("productId", "itemName")

    // Group by productId and count the occurrences
    val resultDf = transactionsDf.groupBy("productId")
      .agg(count("productId").alias("count"))
      .sort("productId")

    // Almost same result
    val resultDF2 = transactionsDf.groupBy("productId").count();

    // Show the result
    resultDf.show()
    resultDF2.show()

    spark.stop()
  }
}
