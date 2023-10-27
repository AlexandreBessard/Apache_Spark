package com.sundogsoftware.spark.udemy.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Test20 {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("SquarePredErrorExample")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // TODO: need to be reviewed

    // Sample data
    val data = Seq(
      (1, "Widget A", "Supplier1"),
      (2, "Widget B", "Supplier2"),
      (3, "Widget C", "Supplier3"),
      (4, "Widget D", "Suppliet4"), // Contains string "et"
      (5, "Widget E", "Supplier5"),
      (6, "Widget F", "Supplier6")
    )

    // Define the schema for the DataFrame
    val schema = List("itemId", "itemName", "supplier")

    // Create a DataFrame from the sample data
    val itemsDf = spark.createDataFrame(data).toDF(schema: _*)

    // Filter rows based on the "supplier" column containing 'et'
    val filteredItemsDf = itemsDf.filter(col("supplier").contains("et"))

    // Show the filtered DataFrame
    filteredItemsDf.show()

    // Stop the SparkSession
    spark.stop()
  }
}
