package com.sundogsoftware.spark.udemy.test3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test22 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("ItemNameSeparatorExample")
      .master("local[*]")
      .getOrCreate()

    // Sample data
    val data = Seq(
      ("Alice", "ProductA", 100),
      ("Alice", "ProductB", 150),
      ("Bob", "ProductA", 200),
      ("Bob", "ProductB", 250)
    )

    // Define the schema for the DataFrame
    val schema = List("name", "product", "value")

    // Create a DataFrame from the sample data
    import spark.implicits._
    val df = data.toDF(schema: _*)

    // Group by "name" and pivot on "product"
    val resultDf = df
      .groupBy("name")
      .pivot("product")
      .sum("value")

    // Show the resulting DataFrame
    resultDf.show()


    // Stop the SparkSession
    spark.stop()
  }
}
