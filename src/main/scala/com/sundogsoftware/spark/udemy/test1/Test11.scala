package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object Test11 {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data for transactionsDf
    val transactionsData = Seq(
      (1, "A", 10),
      (2, "B", 15),
      (3, "C", 20)
    )

    // Sample data for itemsDf
    val itemsData = Seq(
      (1, "Item1"),
      (2, "Item2"),
      (4, "Item4")
    )

    // Define schemas for DataFrames
    val transactionsSchema = StructType(
      StructField("transactionId", IntegerType, nullable = false) ::
        StructField("productId", StringType, nullable = false) ::
        StructField("amount", IntegerType, nullable = false) :: Nil
    )

    val itemsSchema = StructType(
      StructField("itemId", IntegerType, nullable = false) ::
        StructField("itemName", StringType, nullable = false) :: Nil
    )

    // Create DataFrames
    val transactionsDf = spark.createDataFrame(transactionsData).toDF(transactionsSchema.fieldNames: _*)
    val itemsDf = spark.createDataFrame(itemsData).toDF(itemsSchema.fieldNames: _*)

    // Perform an outer join
    val joinedDf = transactionsDf
      .join(itemsDf, transactionsDf("productId") === itemsDf("itemId"), "outer")
    println("outer: ")
    joinedDf.show()

    val joinInner = transactionsDf
      .join(itemsDf, transactionsDf("productId") === itemsDf("itemId"), "inner")
    println("inner: ")
    joinInner.show()

    val joinedDf1 = transactionsDf
      .join(itemsDf, transactionsDf("productId") === itemsDf("itemId"), "right_outer")
    println("right_outer: ")
    joinedDf1.show()

    val joinedDf2 = transactionsDf
      .join(itemsDf, transactionsDf("productId") === itemsDf("itemId"), "left_outer")
    println("left_outer: ")
    joinedDf2.show()

    // Stop the SparkSession
    spark.stop()
  }
}
