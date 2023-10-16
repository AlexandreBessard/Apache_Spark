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

    // TODO: need to be reviewed

    // Sample data
    val data = Seq(
      ("Alice", "ProductA", 100, 1),
      ("Alice", "ProductB", 150, 2),
      ("Bob", "ProductA", 200, 3),
      ("Bob", "ProductB", 250, 4),
      ("Toto", "ProductZ", 1000, 4)
    )

    // Define the schema for the DataFrame
    val schema = List("name", "product", "value", "test")

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
