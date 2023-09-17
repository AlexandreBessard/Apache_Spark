  package com.sundogsoftware.spark.certification.skillcertpro.test2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


  object Test23 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameColumnCasting")
      .master("local[*]") // Change this to your Spark cluster configuration
      .getOrCreate()

    // Sample data
    val data = Seq(
      ("Alice", 28),
      ("Bob", 32),
      ("Charlie", 25)
    )

    // Define the schema
    val schema = List("Name", "Age")

    // Create a DataFrame
    import spark.implicits._
    val df = data.toDF(schema: _*)

    // Register the DataFrame as a temporary SQL table
    df.createOrReplaceTempView("people")

    // Run SQL queries using spark.sql()
    val query1 = "SELECT * FROM people WHERE Age >= 30"
    val result1: DataFrame = spark.sql(query1)

    val query2 = "SELECT Name, Age + 2 AS AdjustedAge FROM people"
    val result2: DataFrame = spark.sql(query2)

    // Show the results
    println("Query 1 Result:")
    result1.show()

    println("Query 2 Result:")
    result2.show()

    // Stop the SparkSession
    spark.stop()

  }
}