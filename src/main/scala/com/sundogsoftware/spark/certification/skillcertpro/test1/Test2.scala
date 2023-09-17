package com.sundogsoftware.spark.certification.skillcertpro.test1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Test2 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)


    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("CustomerDataAnalysis")
      .master("local[*]") // You can specify your Spark cluster master here
      .getOrCreate()

    // Sample DataFrame with customer data
    val customerData = Seq(
      (1, "John", "Doe", "1992-05-15", 101),
      (2, "Jane", "Smith", "1989-08-21", 102),
      (3, "Alice", "Johnson", "1994-03-10", 101),
      (4, "Bob", "Brown", "1990-12-05", 103),
      (5, "Eve", "Williams", "1992-07-18", 102)
    )

    // Define the schema for the DataFrame
    val schema = List(
      "customerId", "first_name", "last_name", "birthdate", "movementID"
    )

    // Create a DataFrame from the sample data and schema
    val customerDF = spark.createDataFrame(customerData).toDF(schema: _*)

    customerDF.summary("count", "mean").show()

    // Stop the SparkSession
    spark.stop()

  }
}
