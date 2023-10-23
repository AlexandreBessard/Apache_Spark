package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Test18 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initializing SparkSession
    val spark = SparkSession.builder()
      .appName("ExplodeAttributesExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //TODO: need to be reviewed

    // Sample data
    val transactionsData = Seq(
      (1, Some(10.5)), // productId, value
      (2, Some(20.0)),
      (3, None),
      (4, Some(40.5)),
      (2, Some(15.0))
    )

    val itemsData = Seq(
      (1, "apple"), // itemId, itemName
      (2, "banana"),
      (3, "cherry"),
      (5, "date")
    )

    val transactionsDf = transactionsData.toDF("productId", "value")
    val itemsDf = itemsData.toDF("itemId", "itemName")

    // Perform the join, filter, and count
    val resultCount = transactionsDf.join(itemsDf,
      transactionsDf.col("productId") === itemsDf.col("itemId"),
      "inner")
      .filter(!col("value").isNull) // Keep rows which are not null
      .count()

    println(s"Number of rows after join and filtering: $resultCount")

    // Closing the SparkSession
    spark.close()
  }
}
