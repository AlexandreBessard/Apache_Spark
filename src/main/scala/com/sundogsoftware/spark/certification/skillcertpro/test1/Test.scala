package com.sundogsoftware.spark.certification.skillcertpro.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Test {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)


    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameExample")
      .master("local[*]") // You can specify your Spark cluster master here
      .getOrCreate()

    // Sample DataFrame with customer data
    val customerData = Seq(
      (1, "John", "Doe", "1992-05-15"),
      (2, "Jane", "Smith", "1989-08-21"),
      (3, "Alice", "Johnson", "1994-03-10")
    )

    // Define the schema for the DataFrame
    val schema = List(
      "customer_id", "first_name", "last_name", "birthdate"
    )

    // Create a DataFrame from the sample data and schema
    val customerDF = spark.createDataFrame(customerData).toDF(schema: _*)

    println(customerDF.show())

    // Define the condition to filter the DataFrame
    val filteredDF = customerDF
      .where(year(col("birthdate")) > 1991 && year(col("birthdate")) < 1993)

    // Count the number of matching records
    val count = filteredDF.count()

    // Show the filtered DataFrame
    filteredDF.show()

    // Show the count of matching records
    println(s"Count of customers born between 1992 and 1993: $count")

    // Stop the SparkSession
    spark.stop()

  }
}
