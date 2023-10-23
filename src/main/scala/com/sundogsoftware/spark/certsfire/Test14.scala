package com.sundogsoftware.spark.certsfire

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, desc, explode}

object Test14 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Initializing SparkSession
    val spark = SparkSession.builder()
      .appName("ExplodeAttributesExample")
      .master("local[*]")
      .getOrCreate()

    // Reading the CSV file into a DataFrame
    val transactionsDf = spark.read
      .option("sep", ";")
      .option("header", "true")
      .option("inferSchema", "true") // By default, spark does not infer the schema.
      .format("csv")
      .load("data/transactions.csv")

    // Display the first few rows of the DataFrame to ensure it's loaded correctly
    transactionsDf.show()

    // Optionally, print the schema
    transactionsDf.printSchema()

    // Closing the SparkSession
    spark.close()
  }
}
