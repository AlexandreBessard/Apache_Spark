package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col  // Required for StringType

object Test10 {

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
      (4, "apple", 4.5, 25), // duplicate entry for demonstration
      (5, "date", 5.0, 26)
    )

    val transactionsDf = transactionsData.toDF("id", "item", "predError", "storeId")

    // Display the original DataFrame (optional)
    println("Original DataFrame:")
    transactionsDf.show()

    // Filtering, selecting specific columns, and removing duplicates
    val resultDf = transactionsDf
      .filter(col("storeId") === 25)
      .select("predError", "storeId")
      .distinct() // predError are distinct even if storeId is the same

    val resultDf1 = transactionsDf
      .filter( $"storeId" === 25) // must be a column when using ===
      .select("predError", "storeId")
      .distinct() // predError are distinct even if storeId is the same
    // distinct based on the "predError" and "storeId"

    // Display the result DataFrame
    println("Filtered and Distinct DataFrame:")
    resultDf.show()
    resultDf1.show()

    // Closing the SparkSession
    spark.close()
  }
}