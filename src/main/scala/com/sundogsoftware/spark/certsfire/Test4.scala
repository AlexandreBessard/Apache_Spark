package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sqrt}  // Corrected the sqrt import here

object Test4 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initializing SparkSession
    val spark = SparkSession.builder()
      .appName("ExplodeAttributesExample")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val transactionsData = Seq(
      (1, "apple", 4.0),
      (2, "banana", 9.0),
      (3, "cherry", 16.0),
      (4, "date", 1.0)
    )

    val transactionsDf = transactionsData.toDF("id", "name", "predError")

    // Display original DataFrame
    println("Original DataFrame:")
    transactionsDf.show()

    // Add a new column "predErrorSqrt" that represents the square root of "predError"
    val transformedDf = transactionsDf.withColumn("predErrorSqrt", sqrt(col("predError")))

    // Display transformed DataFrame
    println("Transformed DataFrame:")
    transformedDf.show()

    // Closing the SparkSession
    spark.close()
  }
}
