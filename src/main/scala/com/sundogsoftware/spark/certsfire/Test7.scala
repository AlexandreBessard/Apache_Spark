package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType  // Required for StringType

object Test7 {

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
      (1, 101),
      (2, 102),
      (3, 103),
      (4, 104)
    )

    val transactionsDf = transactionsData.toDF("id", "storeId")

    // Display original DataFrame
    println("Original DataFrame:")
    transactionsDf.show()

    // Casting storeId to StringType
    val transformedDf =
      transactionsDf.select(col("id"), col("storeId").cast(StringType))

    val transformedDf1 =
      transactionsDf.select(col("id"), col("storeId"));

    val transformedDf2 =
      transactionsDf.select("id", "storeId");

    // Display transformed DataFrame
    println("Transformed DataFrame:")
    transformedDf.show()

    // Closing the SparkSession
    spark.close()
  }
}